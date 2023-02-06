import * as dotenv from 'dotenv'
import { Transform } from "stream"
import { pipeline } from 'stream/promises'
import { createReadStream, createWriteStream } from 'fs'
import ndjson from 'ndjson'
import { networkInterfaces } from 'os'

const main = async () => {
  dotenv.config()
  const savePath = process.env.SAVE_PATH_CSV
  const readPath = process.env.READ_PATH_JSON
  const readStream = createReadStream(readPath)
  const writeFileSync = createWriteStream(savePath)

  const flattenJSON = function (obj = [], res = {}, extraKey = '') {
    for (var key in obj) {
      if (typeof obj[key] !== 'object') {
        res[extraKey + key] = obj[key];
      } else {
        flattenJSON(obj[key], res, `${extraKey}${key}.`);
      }
    }
    return JSON.stringify(res)
  }

  const myTransform = new Transform({
    objectMode: true,
    transform(chunk, enc, cb) {
      const returno = flattenJSON(chunk) + "\n"
      cb(null, returno)
      console.log('>>>>>DATA', flattenJSON(chunk))
    },
  })

  const accessData = new Transform({
    transform(chunk, enc, cb){
      const data = JSON.parse(chunk)
      const retorno = `${data['event_date']},${data['event_timestamp']},${data['event_name']},${data['event_params.0.key']},${data['event_params.0.value.int_value']},${data['event_params.1.key']},${data['event_params.1.value.int_value']},${data['event_params.2.key']},${data['event_params.2.value.string_value']},${data['event_params.3.key']},${data['event_params.3.value.string_value']},${data['event_params.4.key']},${data['event_params.4.value.int_value']},${data['event_params.5.key']},${data['event_params.5.value.string_value']},${data['event_params.6.key']},${data['event_params.6.value.int_value']},${data['event_bundle_sequence_id']},${data['user_pseudo_id']},${data['privacy_info.uses_transient_token']},${data['user_first_touch_timestamp']},${data['user_ltv.revenue']},${data['user_ltv.currency']},${data['device.category']},${data['device.mobile_brand_name']},${data['device.mobile_model_name']},${data['device.operating_system']},${data['device.operating_system_version']},${data['device.language']},${data['device.is_limited_ad_tracking']},${data['device.web_info.browser']},${data['device.web_info.browser_version']},${data['device.web_info.hostname']},${data['geo.continent']},${data['geo.country']},${data['geo.region']},${data['geo.city']},${data['geo.sub_continent']},${data['geo.metro']},${data['traffic_source.name']},${data['traffic_source.medium']},${data['traffic_source.source']},${data['stream_id']},${data['platform']},\n`
      cb(null, retorno)
    }
  })

  const setCSVHeader = new Transform({
    transform(chunk, env, cb){
      this.counter = this.counter ?? 0
      if(this.counter > 0){
        return cb(null,chunk)
      }
      this.counter += 1
      const header = `event_date,event_timestamp,event_name,event_params.0.key,event_params.0.value.int_value,event_params.1.key,event_params.1.value.int_value,event_params.2.key,event_params.2.value.string_value,event_params.3.key,event_params.3.value.string_value,event_params.4.key,event_params.4.value.int_value,event_params.5.key,event_params.5.value.string_value,event_params.6.key,event_params.6.value.int_value,event_bundle_sequence_id,user_pseudo_id,privacy_info.uses_transient_token,user_first_touch_timestamp,user_ltv.revenue,user_ltv.currency,device.category,device.mobile_brand_name,device.mobile_model_name,device.operating_system,device.operating_system_version,device.language,device.is_limited_ad_tracking,device.web_info.browser,device.web_info.browser_version,device.web_info.hostname,geo.continent,geo.country,geo.region,geo.city,geo.sub_continent,geo.metro,traffic_source.name,traffic_source.medium,traffic_source.source,stream_id,platform\n`.concat(chunk)
      cb(null, header)

    }
  })


  try {
    await pipeline(
      readStream,
      ndjson.parse(),
      myTransform,
      accessData,
      setCSVHeader,
      writeFileSync
    )

    console.log('Stream ended')
  } catch (error) {
    console.error("Stream ended error", error)
  }



}
main()
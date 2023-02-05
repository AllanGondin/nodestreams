import {Transform } from "stream"
import {pipeline} from 'stream/promises'
import { createReadStream, createWriteStream} from 'fs'
import ndjson from 'ndjson'
import csvjson from "csvjson"

const main = async () => {
  const readStream = createReadStream('./files/test.json')
  const writeFileSync = createWriteStream('./files/testFilter.csv')

  const flattenJSON = function(obj = [], res = {}, extraKey = '') {
    for (var key in obj) {
      if (typeof obj[key] !== 'object') {
        res[extraKey + key] = obj[key];
      }else{
        flattenJSON(obj[key], res, `${extraKey}${key}.`);
      }
    }
    return JSON.stringify(res)
  }

  let line = 0;

  const myTransform = new Transform({
    objectMode: true,
    transform(chunk, enc, cb) {

      const returno = flattenJSON(chunk)
      cb(null, returno)
      console.log('>>>>>DATA', flattenJSON(chunk))
    },
  })

  const transformJsonToCSV = new Transform({
    transform(chunk, enc, cb) {
        const csv = csvjson.toCSV(chunk, {
          headers: ("name","test1","Test.Aaa")
      })
        cb(null,csv);
    },
    readableObjectMode: true,
    writableObjectMode: true,
});

try {
  await pipeline(
    readStream,
    ndjson.parse(),
    myTransform,
    transformJsonToCSV,
    writeFileSync
  )
  
  console.log('Stream ended')
  
} catch (error) {
  console.error("Stream ended error", error)
}



}
main()
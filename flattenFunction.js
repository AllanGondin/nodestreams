const flattenJSON = (obj = [], res = {}, extraKey = '') => {
    for (var key in obj) {
      if (typeof obj[key] !== 'object') {
        res[extraKey + key] = obj[key];
      }else{
        flattenJSON(obj[key], res, `${extraKey}${key}.`);
      }
    }
    return res
  }

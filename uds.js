function transform(line) {
    var values = line.split(',');
    var obj = new Object();
    obj.country = values[0];
    obj.region = values[1];
    obj.province = values[2];
    obj.confirmed_cases = values[3];
    obj.deaths = values[4];
    obj.recovered = values[5];
    obj.fatality_rate = values[6];
    var jsonString = JSON.stringify(obj);
    return jsonString;
   }
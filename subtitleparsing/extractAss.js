const fs = require('node:fs');
const path = require("node:path");
const ass = require('./ass-parser/index.js');
const { Client } = require("pg");

const client = new Client({
  user: 'jpcodes',
  password: 'JpCodes',
  host: '127.0.0.1',
  port: 5432,
  database: 'jpcodes',
});

function walk(dir, callback) {
  const entires = fs.readdirSync(dir)
  for (const file of entires){
    const fp = path.join(dir, file);
    const stat = fs.statSync(fp);
    if (stat.isDirectory()){
      walk(fp, callback) 
    }
    else{
     callback(fp); 
    }
  }
}

let output = {}
const filterNonAsian = /[^\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}]+/gu

const hasKana = /[\p{Script=Hiragana}\p{Script=Katakana}]/u;
function identifyNonJp(obj){

  if ('Style' in obj){

    if (obj.style.includes("CN") || obj.style.includes("ZH")) {
    return false;
    }

  }

  if (!obj.value.Text){return false; };
  if (obj == undefined){return false};
  if (obj.value.Text.includes('CN')){return false}
  if (obj.value.Text.includes('EN')){return false}
  if (obj.value.Text.includes('ZH')){return false}

  return hasKana.test(obj.value.Text);
}

let i = 0;

(async () => {
  try {
    await client.connect();
    console.log("Connected successfully");
  } catch (err) {
    console.error("Connection error", err.stack);
  }  
})().then( async () => {

  console.log("!!");

  walk("../sources/subtitles/",  (x) => {
      if (!x.includes(".ass")){return;}
      let text = []
      const buff = fs.readFileSync(x);
      const data = buff.toString();

      let parsed = ass(data).filter(x=> x.section == "Events");
       
      // Some can have multiple Event blocks;
      if (parsed.length == 0){
        return
      } 


      let result = ""
      for (const event in parsed){
        let dialogueText = parsed[event].body.filter(x=> x.key == 'Dialogue').filter(x=>identifyNonJp(x)).map(x=> {
            return x.value.Text.replaceAll(filterNonAsian, '');
        }).filter(x=> x.length > 1).join(" ");
  result += ` ${dialogueText}`
      }

      const abs = path.resolve(x);
      console.log(abs);
      output[abs] = result;
      i++ 
      console.log(`The Total: ${i}`)
  });

  console.log(output);

  await client.query("BEGIN")
  for (const path in output) {
    const r = await client.query(`INSERT INTO training_data.SubtitleData(data, path) VALUES ($1::text, $2::text);`, 
    [output[path], path])

    console.log(r);
  }

  await client.query("COMMIT");
  client.end(); 
});

const ass = require('.');
const fs = require('fs');
const path = require("path");


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

function walkSync(dir, callback) {
    const entries = fs.readdirSync(dir);
    for (const entry of entries) {
        const filepath = path.join(dir, entry);
        const stats = fs.statSync(filepath);

        if (stats.isDirectory()) {
            walkSync(filepath, callback);
        } else if (stats.isFile()) {
            callback(filepath, stats);
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

walk("../../sources/subtitles/", (x) => {
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

    output[x] = result

});

console.log(output);

const gnomedb = require('./modules/gnomedb');
const fs = require('fs');
var parse = require('csv-parse/lib/sync');

function Record(name, prim_cat, oth_cat) {
    this.name = name;
    this.sku = null;
    this.price = null;
    this.primary_category = prim_cat;
    this.other_categories = oth_cat;
    this.flags = null;
    this.tags = null;
    this.primary_photo = null;
    this.other_photos = null;
    this.short_description = null;
    this.long_description = null;
    this.package_quantity = null;
    this.weight = null;
    this.length = null;
    this.width = null;
    this.height = null;
    this.bulk = null;
    this.inventory_par = null;
    this.inventory_low = null;
    this.inventory = null;
    this.last_recieved = null;
    this.last_inventory = null;
    this.last_sold = null;
}

const flags = ["Core", "Brewing Basics", "Special Order Only", "Temporarily Unavailable", "Discontinued", "Hidden"];
const tags = ["red", "white", "blue"];
const bulk = [false, true]

const data = fs.readFileSync('Inventory.csv');
const rows = parse(data, {columns: true, trim: true});

const inventory = [];

for (var item = 0; item < rows.length; item++) {
    var inv_item = {};

    inv_item.primary_category = rows[item]['Primary Category'];

    if (inv_item.primary_category == "Ingredients") {
        inv_item.grocery = true;
    } else {
        inv_item.grocery = false;
    }

    var other = rows[item]['Other Categories'].slice(1,-1);
    inv_item.other_categories  = parse(other,{delimiter:'?', trim: true})[0];

    inv_item.name = rows[item]['Name'].replace(/\*/g,',');

    // const inv_item = new Record(name, prim_cat, oth_cat);

    inv_item.sku = Math.floor(Math.random() * 100000) + 1
    inv_item.price = Math.floor(Math.random() * 100000) / 100.0

    var index = Math.floor(Math.random() * 6);
    inv_item.flags = [];
    inv_item.flags.push(flags[index]);

    index = Math.floor(Math.random() * 3);
    inv_item.tags = [];
    inv_item.tags.push(tags[index]);

    inv_item.weight = Math.floor(Math.random()* 100 + 5) / 10
    inv_item.length = Math.floor(Math.random() * 90 + 10) / 10
    inv_item.width = Math.floor(Math.random() * 90 + 10) / 10
    inv_item.height = Math.floor(Math.random() * 90 + 10) / 10
    
    index = Math.floor(Math.random() * 2)
    inv_item.bulk = bulk[index];
    
    inv_item.last_recieved = new Date(2018,0,31);
    inv_item.last_inventory = new Date(2017,11,31)
    inv_item.last_sold = new Date(2018,0,5)

    inv_item.inventory_low = 10;
    inv_item.inventory_par = 100;
    inv_item.inventory = Math.floor(Math.random() * 70) + 20;

    inventory.push(inv_item);
}
/*
(async () => {
    try {
        let ops = await gnomedb.insertMany('inventory', inventory);
    } catch(error) {
        console.log(error.message);
    }
})();
*/
(async () => {
    let db_con;
    let result;
  
    try {
      db_con = await gnomedb.connect_server();
  
      // Access production database
      let db = db_con.client.db(db_con.targetDB);
  
      // Select collection for insert
      let col = db.collection('inventory');

      // Drop existing inventory collection
      result = await col.drop();
  
      // Insert test inventory documents
      result = await col.insertMany(inventory);
    } catch (err) {
      console.log(err.message);
    } finally {
      db_con.client.close();
    }
})();
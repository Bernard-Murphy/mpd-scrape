const { MongoClient } = require("mongodb");
const axios = require("axios");
const dotenv = require("dotenv");
const { parse } = require("node-html-parser");
const crypto = require("crypto");
const app = require("express")();

dotenv.config();

const port = process.env.PORT;
const mongoUrl = process.env.MONGO_URL;
const client = new MongoClient(mongoUrl);
const interval = 1000 * 60 * 15;

const scrape = () => {
  console.log("Scraping", new Date());
  axios
    .get(process.env.MKE_URL)
    .then(async (res) => {
      try {
        const items = Array.from(parse(res.data).getElementsByTagName("tr"));
        const db = client.db("mke-logs");
        for (let i = 0; i < items.length; i++) {
          const item = items[i];
          const cells = Array.from(item.getElementsByTagName("td"))
            .map((cell) => cell.textContent)
            .filter((cell) => cell);

          const hash = crypto
            .createHash("md5")
            .update(JSON.stringify(cells))
            .digest("hex");
          const check = await db.collection("logs").findOne({ hash: hash });
          if (!check)
            await db.collection("logs").insertOne({
              _id: crypto.randomBytes(8).toString("hex"),
              hash: hash,
              callNumber: cells[0],
              dateTime: cells[1],
              location: cells[2],
              policeDistrict: cells[3],
              natureOfCall: cells[4],
              status: cells[5],
            });
        }
      } catch (err) {
        console.log("Data parse error", err);
      }
    })
    .catch((err) => {
      console.log("err", err);
    })
    .finally(() => setTimeout(scrape, interval));
};

app.listen(port, () => {
  scrape();
  console.log("MPD scraper running on port", port);
});

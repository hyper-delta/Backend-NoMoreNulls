const express = require("express");
const cors = require("cors");
require("dotenv").config();
const metadataRoutes = require("./routes/metadata");

const app = express();

app.use(cors());
app.use(express.json());

app.use("/api/metadata", metadataRoutes);

const PORT = process.env.PORT || 5000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

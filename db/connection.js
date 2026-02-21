const mysql = require("mysql2/promise");

const createConnection = async (config) => {
  return await mysql.createConnection({
    host: config.host,
    user: config.user,
    password: config.password,
    database: config.database,
  });
};

module.exports = { createConnection };

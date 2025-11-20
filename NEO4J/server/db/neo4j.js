const neo4j = require('neo4j-driver');

const uri = process.env.NEO4J_URI;
const user = process.env.NEO4J_USER;
const password = process.env.NEO4J_PASSWORD;

if (!uri || !user || !password) {
  throw new Error('NEO4J_URI, NEO4J_USER y NEO4J_PASSWORD son requeridos');
}

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));

const getSession = (database) => driver.session({ database });

const closeDriver = async () => {
  await driver.close();
};

process.on('exit', () => {
  closeDriver().catch((err) => console.error('Error cerrando driver', err));
});
process.on('SIGINT', () => {
  closeDriver().catch((err) => console.error('Error cerrando driver', err));
  process.exit(0);
});

module.exports = {
  driver,
  getSession,
  neo4j,
};

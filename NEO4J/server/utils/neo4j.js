const { isInt } = require('neo4j-driver');

const toNative = (value) => {
  if (isInt(value)) {
    return value.toNumber();
  }

  if (Array.isArray(value)) {
    return value.map(toNative);
  }

  if (value && typeof value === 'object') {
    if (typeof value.toString === 'function' && value.toString() !== '[object Object]') {
      return value.toString();
    }

    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, toNative(v)]));
  }

  return value;
};

const serializeNode = (node) => ({
  ...toNative(node.properties),
  labels: node.labels,
});

module.exports = {
  toNative,
  serializeNode,
};

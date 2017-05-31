const _ = require('lodash');
const encode = require('hashcode').hashCode().value;
           
function create() {
    const dictionary = {};
    const lookup = {};

    const set = (k, v) => {
        const hash = String(encode(k));
        lookup[hash] = k;
        _.set(dictionary, hash, v);
    };

    const get = (k) => {
        const hash = String(encode(k));
        return _.get(dictionary, hash);
    };

    const value = () => {
        return _.cloneDeep(dictionary);
    };

    const lookupHash = (h) => {
        return _.get(lookup, h);
    };

    return { set, get, lookupHash, value };
}

module.exports = { create };

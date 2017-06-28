const _ = require('lodash');

function isUDPPort(p) {
    return _.endsWith(p, "/udp");
}

module.exports = {
    isUDPPort
}

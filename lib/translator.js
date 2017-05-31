const _ = require('lodash');
const ComplexDictionary = require('./complex-dictionary');

const CS_TO_K8S_POD_MAPPING = ComplexDictionary.create();
let defineMapping = CS_TO_K8S_POD_MAPPING.set;
defineMapping(['id'],             [['containers', 0, 'name']]);
defineMapping(['name'],           [['containers', 0, 'name']]);
defineMapping(['env_vars'],       [['containers', 0, 'env']]);
defineMapping(['privileged'],     [['containers', 0, 'securityContext', 'privileged']]);
defineMapping(['image'],          [['containers', 0, 'image']]);
defineMapping(['cpus'],           [['containers', 0, 'resources', 'limits', 'cpu']]);
defineMapping(['memory'],         [['containers', 0, 'resources', 'limits', 'memory']]);
defineMapping(['command'],        [['containers', 0, 'command']]);
defineMapping(['privileged'],     [['containers', 0, 'securityContext', 'privileged']]);
defineMapping(['respawn'],        [['containers', 0, 'restartPolicy']]);
defineMapping(['host_port'],      [['containers', 0, 'ports', 0]]);
defineMapping(['container_port'], [['containers', 0, 'ports', 0]]);
defineMapping(['volumes'],        [['containers', 0, 'volumeMounts'], ['volumes']]);
defineMapping(['network_mode'],   [['hostNetwork']]);

const CS_TO_K8S_RC_MAPPING = ComplexDictionary.create();
defineMapping = CS_TO_K8S_RC_MAPPING.set;
defineMapping(['id'], [['metadata', 'name'], ['spec', 'template', 'metadata', 'labels', 'app']]);
defineMapping(['tags'], [['metadata', 'labels']]);
defineMapping(['name'], [['metadata', 'labels', 'app']]);
defineMapping(['count'], [['spec', 'replicas']]);

const CS_TO_K8S_NODE_MAPPING = ComplexDictionary.create();
defineMapping = CS_TO_K8S_NODE_MAPPING.set;
defineMapping(['id'],                 ['metadata', 'labels', 'cs-node-id']);
defineMapping(['host_name'],          ['metadata', 'name']);
defineMapping(['last_sync'],          ['status', 'conditions', 2, 'lastHeartbeatTime']);
defineMapping(['state'],              ['status', 'conditions', 2, 'status']);
defineMapping(['start_time'],         ['metadata', 'creationTimestamp']);
defineMapping(['address', 'public'],  ['status', 'addresses', 0]);
defineMapping(['address', 'private'], ['status', 'addresses', 1]);
defineMapping(['cpus'],               ['status', 'capacity', 'cpu']);
defineMapping(['memory'],             ['status', 'capacity', 'memory']);

const CS_TO_K8S_CONVERSIONS = ComplexDictionary.create();
let defineConversion = CS_TO_K8S_CONVERSIONS.set;
defineConversion(['command'],        (v) => v ? _.split(v, ' ') : []);
defineConversion(['privileged'],     (v) => v === 'true');
defineConversion(['host_port'],      (v) => ({'hostPort': v}));
defineConversion(['container_port'], (v) => ({'containerPort': v}));
defineConversion(['network_mode'],   (v) => v === 'host' ? true : false);

defineConversion(['env_vars'], (v) => v ? _.map(v, (value, name) => {
    return { name: name, value: String(value) };
}) : []);

defineConversion(['cpus'], (v, destinationPath) => {

    if(!v) return null;

    if(_.includes(destinationPath, 'capacity')) {
        return v;
    } else {
        return `${v * 1000}m`
    }


});

defineConversion(['memory'], (v, destinationPath) => {

    if(!v) return null;

    if(_.includes(destinationPath, 'capacity')) {
        _.flow(
            _.partial(_.replace, _, 'M', ''),
            _.partial(_.parseInt),
            (v) => v * 1000,
            (v) => `${v}Ki`)(v);
    } else {
        return `${v}M`;
    }

});

//Look here, need proper volume mounting.
defineConversion(['volumes'], (volumes, destinationPath) => {

    const pathToName = (p) => p !== '/' ? _.replace(p, new RegExp('/', 'g'), '') : 'root';

    return _.includes(destinationPath, 'containers') ?
        _.map(volumes, (v) => ({
            name: pathToName(v.host),
            mountPath: v.container
        })) :
        _.map(volumes, (v) => ({
            name: pathToName(v.host),
            hostPath: {
                path: v.host
            }
        }));

});

defineConversion(['tags'], (v) =>  _.flow(
    _.partial(_.get, _, ['constraints'], {per_host: 0}),
    _.partial(_.mapKeys, _, (v, k) => `tags.constraints.${k}`),
    _.partial(_.mapValues, _, (v, k) => `${v}`)
)(v));

const K8S_TO_CS_CONVERSIONS = ComplexDictionary.create();
defineConversion = K8S_TO_CS_CONVERSIONS.set;
defineConversion(['command'],            (v) => v ? v.join(' ') : null)
defineConversion(['state'],              (v) => v ? _.get({'True': 'operational'}, v, 'unknown-state') : null);
defineConversion(['address', 'public'],  (v) => v ? v.address : null);
defineConversion(['address', 'private'], (v) => v ? v.address : null);
defineConversion(['host_port'],          (v) => v ? v.hostPort : null);
defineConversion(['container_port'],     (v) => v ? v.containerPort : null);
defineConversion(['respawn'],            (v) => v ? v === 'Always' : false);
defineConversion(['network_mode'],       (v) => v ? 'host' : 'bridge');

defineConversion(['env_vars'], (envVars) => {
    return _.merge.apply(null,
        _.map(envVars, (v) => {
            return _.set({}, v.name, v.value);
        }), {});
});

defineConversion(['cpus'], (v, sourcePath) =>  {

    if(!v) return null;

    //Conversion for host
    if(_.includes(sourcePath, 'capacity')) {
        return v;
    } else {//Conversion for container spec
        return _.flow(
            _.partialRight(_.replace, 'm', ''),
            parseInt,
            (v) => v / 1000)(v);
    }

});

defineConversion(['memory'],  (v, sourcePath) => {
    if(!v) return null;

    if(_.includes(sourcePath, 'capacity')) {
        return parseInt(_.replace(v, 'Ki', '')) * 1000;
    } else {
        return parseInt(_.replace(v, ('M', '')));
    }
});

defineConversion(['volumes'], (volumeMounts, srcPath0, volumeDests, srcPath1) => {
    const volumes = _.zipWith(volumeMounts, volumeDests, _.merge);
    return _.map(volumes, (v) => ({
        host: v.hostPath.path,
        container: v.mountPath
    }));
});

defineConversion(['tags'], (v) =>  _.flow(
    _.partial(_.pickBy, _, (v, k) => _.startsWith(k, 'tags')),
    _.partial(_.mapKeys, _, (v, k) => _.replace(k, 'tags.', "")),
    _.partial(_.reduce, _, (csTags, v, k) => (_.set(csTags, k, v)), {}) 
)(v));

function csApplicationToK8SPodSpec(csAppDesc) { 
    return _.reduce(CS_TO_K8S_POD_MAPPING.value(), (k8sApp, k8sPaths, csKeyHash) => {

        const csKey = CS_TO_K8S_POD_MAPPING.lookupHash(csKeyHash);
        const csValue = _.get(csAppDesc, csKey);
        const conversionFn = CS_TO_K8S_CONVERSIONS.get(csKey) || _.identity;

        if(csValue) {
            const updatedPaths = _.reduce(k8sPaths, (setK8SPaths, path) => {
                const currentValue = _.get(setK8SPaths, path);
                const newValue = conversionFn(csValue, path);

                return _.merge(setK8SPaths, _.set({}, path, newValue));
            }, {});
            
            return _.merge(k8sApp, updatedPaths);
        } else {
            return k8sApp;
        }

    }, {});
}

function csApplicationToK8SReplicationController(csAppDesc) {
    const podSpec = csApplicationToK8SPodSpec(csAppDesc);

    return _.reduce(CS_TO_K8S_RC_MAPPING.value(), (k8sRC, k8sPaths, csKeyHash) => {
        const csKey = CS_TO_K8S_RC_MAPPING.lookupHash(csKeyHash);
        const csValue = _.get(csAppDesc, csKey, null);
        const conversionFn = CS_TO_K8S_CONVERSIONS.get(csKey) || _.identity;

        console.log(csValue + " for " + csKey);

        if(csValue != null) {
            const updatedPaths = _.reduce(k8sPaths, (setK8SPaths, path) => {
                const currentValue = _.get(setK8SPaths, path);
                const newValue = conversionFn(csValue, path);

                return _.merge(setK8SPaths, _.set({}, path, newValue));
            }, {});

            return _.merge(k8sRC, updatedPaths);
        } else {
            return k8sRC;
        }
    }, {
        "kind": "ReplicationController",
        'spec': {
            'template': {
                'spec': podSpec
            }
        }
    });
}

function csApplicationFromK8SPodSpec(k8sPodSpec) {
    _.reduce(CS_TO_K8S_POD_MAPPING.value(), (csApp, k8sPaths, csKeyHash) => {
        const csKey = CS_TO_K8S_POD_MAPPING.lookupHash(csKeyHash);
        const k8sValues = _.map(k8sPaths, (k8sPath) => _.get(k8sApp, k8sPath));
        const conversionFn = K8S_TO_CS_CONVERSIONS.get(csKey) || _.identity;

        const newValue = conversionFn && k8sValues ?
            conversionFn.apply(null, _.flatten(_.zip(k8sValues, k8sPaths))) :
            _.first(k8sValues);

        return !_.isNil(v) ? _.set(csApp, csKey, newValue) : csApp;

    }, {});
}

function csHostFromK8SNode(k8sNode) {
    return _.reduce(CS_TO_K8S_NODE_MAPPING.value(), (csNode, k8sPath, hashCSPath) => {
        const csPath = CS_TO_K8S_NODE_MAPPING.lookupHash(hashCSPath);
        const converionFn = K8S_TO_CS_CONVERSIONS.get(csPath) || _.identity;
        const newValue = converstionFn(_.get(k8sNode, k8sPath), k8sPath);

        return newValue ? _.set(csNode, csPath, newValue) : csNode;
    }, {});
}


module.exports = { 
    csApplicationToK8SPodSpec, 
    csApplicationToK8SReplicationController, 
    csApplicationFromK8SPodSpec, 
    csHostFromK8SNode
};

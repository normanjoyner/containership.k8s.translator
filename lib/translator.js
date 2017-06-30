const _ = require('lodash');
const ComplexDictionary = require('./complex-dictionary');
const Util = require('./util');

const CS_TO_K8S_POD_MAPPING = ComplexDictionary.create();
let defineMapping = CS_TO_K8S_POD_MAPPING.set;
defineMapping(['id'],             [['containers', 0, 'name']]);
defineMapping(['name'],           [['containers', 0, 'name']]);
defineMapping(['env_vars'],       [['containers', 0, 'env']]);
defineMapping(['image'],          [['containers', 0, 'image']]);
defineMapping(['cpus'],           [['containers', 0, 'resources', 'limits', 'cpu']]);
defineMapping(['memory'],         [['containers', 0, 'resources', 'limits', 'memory']]);
defineMapping(['command'],        [['containers', 0, 'command']]);
defineMapping(['host_port'],      [['containers', 0, 'ports', 0]]);
defineMapping(['container_port'], [['containers', 0, 'ports', 0]]);
defineMapping(['volumes'],        [['containers', 0, 'volumeMounts'], ['volumes']]);
defineMapping(['privileged'],     [['securityContext', 'privileged']]);
defineMapping(['respawn'],        [['restartPolicy']]);
defineMapping(['network_mode'],   [['hostNetwork']]);

const CS_TO_K8S_RC_MAPPING = ComplexDictionary.create();
defineMapping = CS_TO_K8S_RC_MAPPING.set;
defineMapping(['id'],     [['metadata', 'name'], ['spec', 'template', 'metadata', 'labels', 'app']]);
defineMapping(['tags'],   [['metadata', 'labels']]);
defineMapping(['name'],   [['metadata', 'labels', 'app']]);
defineMapping(['engine'], [['metadata', 'labels', 'engine']]);
defineMapping(['count'],  [['spec', 'replicas']]);

const CS_TO_K8S_NODE_MAPPING = ComplexDictionary.create();
defineMapping = CS_TO_K8S_NODE_MAPPING.set;
defineMapping(['id'],                 [['metadata', 'labels', 'cs-node-id']]);
defineMapping(['host_name'],          [['metadata', 'name']]);
defineMapping(['last_sync'],          [['status', 'conditions', 2, 'lastHeartbeatTime']]);
defineMapping(['state'],              [['status', 'conditions', 2, 'status']]);
defineMapping(['start_time'],         [['metadata', 'creationTimestamp']]);
defineMapping(['address', 'public'],  [['status', 'addresses', 0, 'address']]);
defineMapping(['address', 'private'], [['status', 'addresses', 1, 'address']]);
defineMapping(['cpus'],               [['status', 'capacity', 'cpu']]);
defineMapping(['memory'],             [['status', 'capacity', 'memory']]);

const CS_TO_K8S_CONVERSIONS = ComplexDictionary.create();
let defineConversion = CS_TO_K8S_CONVERSIONS.set;
defineConversion(['command'],        (v) => v ? _.split(v, ' ') : []);
defineConversion(['privileged'],     (v) => v === 'true');
defineConversion(['respawn'],        (v) => v ? 'Always' : 'Never');
defineConversion(['network_mode'],   (v) => v === 'host' ? true : false);
defineConversion(['privileged'],     (v) => String(v));

defineConversion(['host_port'], (v) => {
    return {
        'hostPort': _.head(_.split(v, "/")),
        'protocol': Util.isUDPPort(v) ? "UDP" : "TCP"
    };
});

defineConversion(['container_port'], (v) => {
    return {
        'containerPort': _.head(_.split(v, "/")),
        'protocol': Util.isUDPPort(v) ? "UDP" : "TCP"
    }
});

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
            name: pathToName(v.container),
            mountPath: v.container
        })) :
        _.map(volumes, (v) => ({
            name: pathToName(v.container),
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
defineConversion(['address', 'public'],  (v) => v ? v : null);
defineConversion(['address', 'private'], (v) => v ? v : null);
defineConversion(['respawn'],            (v) => v ? v === 'Always' : false);
defineConversion(['network_mode'],       (v) => v ? 'host' : 'bridge');
defineConversion(['privileged'],         (v) => v === 'true');
defineConversion(['start_time'],         (v) => Date.parse(v))

defineConversion(['host_port'], (v) => {
    if(v.hostPort) {
        return `${v.hostPort}${_.upperCase(v.protocol) === 'UDP' ? "/udp" : ""}`;
    }

    // TODO support returning undefined.
    return null;
});

defineConversion(['container_port'], (v) => {
    if(v.containerPort) {
        return `${v.containerPort}${_.upperCase(v.protocol) === 'UDP' ? "/udp" : ""}`;
    }

    return null;
});

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
        return Math.floor(parseInt(_.replace(v, 'Ki', '')) * 1000);
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

function convertTo(mapping, conversion, source, initial) {
    initial = initial || {};

    return _.reduce(mapping.value(), (destination, destinationPaths, sourceKeyHash) => {
        const sourceKey = mapping.lookupHash(sourceKeyHash);
        const sourceValue = _.get(source, sourceKey);
        const conversionFn = conversion.get(sourceKey) || _.identity;

        if(!_.isNil(sourceValue)) {
            const updatedPaths = _.reduce(destinationPaths, (setPaths, path) => {
                const currentValue = _.get(setPaths, path);
                const newValue = conversionFn(sourceValue, path);

                return _.merge(setPaths, _.set({}, path, newValue));
            }, {});

            return _.merge(destination, updatedPaths);
        } else {
            return destination;
        }
    }, initial);
}

function convertFrom(mapping, conversion, source, initial) {
    initial = initial || {};

    return _.reduce(mapping.value(), (destination, sourcePaths, destinationKeyHash) => {
        const destinationKey = mapping.lookupHash(destinationKeyHash);
        const sourceValues = _.map(sourcePaths, (p) => _.get(source, p));
        const conversionFn = conversion.get(destinationKey) || _.identity;
        const newValue = conversionFn.apply(null, _.flatten(_.zip(sourceValues, sourcePaths)));

        return !_.isNil(newValue) ?
            _.set(destination, destinationKey, newValue) :
            destination;

    }, initial);

}

function csApplicationToK8SPodSpec(csAppDesc) {
    return convertTo(CS_TO_K8S_POD_MAPPING, CS_TO_K8S_CONVERSIONS, csAppDesc, {
        imagePullPolicy: 'Always'
    });
}

function csApplicationToK8SReplicationController(csAppDesc) {
    const podSpec = csApplicationToK8SPodSpec(csAppDesc);

    return convertTo(CS_TO_K8S_RC_MAPPING, CS_TO_K8S_CONVERSIONS, csAppDesc, {
        kind: 'ReplicationController',
        spec: {
            template: {
                spec: podSpec
            }
        }
    });
}

function csApplicationFromK8SPodSpec(k8sPodSpec) {
    return convertFrom(CS_TO_K8S_POD_MAPPING, K8S_TO_CS_CONVERSIONS, k8sPodSpec);
}

function csApplicationFromK8SReplicationController(k8sRC) {
    const podSpec = _.get(k8sRC, ['spec', 'template', 'spec']);
    return _.merge(
        csApplicationFromK8SPodSpec(podSpec),
        convertFrom(CS_TO_K8S_RC_MAPPING, K8S_TO_CS_CONVERSIONS, k8sRC));
}

function csHostFromK8SNode(k8sNode) {
    return convertFrom(CS_TO_K8S_NODE_MAPPING, K8S_TO_CS_CONVERSIONS, k8sNode);
}

const PHASE_MAP = {
    Pending: 'loading',
    Running: 'loaded',
    Succeeded: 'unloaded',
    Failed: 'unloaded',
    Unknown: 'unloaded'
};

function csStatusFromK8SStatus(status) {
    return PHASE_MAP[status && status.phase] || 'unloaded';
}

module.exports = {
    csApplicationToK8SPodSpec,
    csApplicationToK8SReplicationController,
    csApplicationFromK8SPodSpec,
    csApplicationFromK8SReplicationController,
    csHostFromK8SNode,
    csStatusFromK8SStatus
};


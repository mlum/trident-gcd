package trident.gcd;

import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Values;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

import com.google.api.services.datastore.DatastoreV1.*;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.protobuf.ByteString;

public class GCDState<T> implements IBackingMap<T> {
    private final Datastore _datastore;
    private Options _opts;
    private Serializer _ser;

    private static final Map<StateType, Serializer> DEFAULT_SERIALIZERS = new HashMap<StateType, Serializer>() {{
        put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }};

    public static class Options<T> implements Serializable {
        public int localCacheSize = 1000;
        public String globalKey = "$__GLOBAL_KEY__$";
        public Serializer<T> serializer = null;
        public String datastoreKind = "TridentState";
        public String datastoreProperty = "state";
    }

    protected static class Factory implements StateFactory {
        StateType _type;
        String _datasetId;
        Serializer _ser;
        Options _opts;

        public Factory(String datasetId, StateType type, Options options) {
            _type = type;
            _datasetId = datasetId;
            _opts = options;
            if(options.serializer == null) {
                _ser = DEFAULT_SERIALIZERS.get(type);
                if(_ser == null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            } else {
                _ser = options.serializer;
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public State makeState(Map conf, IMetricsContext context, int partitionIndex, int numPartitions) {
            GCDState s;
            try {
                s = new GCDState(makeDatastoreClient(_opts, _datasetId), _opts, _ser);
            } catch(GeneralSecurityException ge) {
                throw new RuntimeException(ge);
            } catch(IOException ie) {
                throw new RuntimeException(ie);
            }
            CachedMap c = new CachedMap(s, _opts.localCacheSize);
            MapState mapState;
            if(_type == StateType.NON_TRANSACTIONAL) {
                mapState = NonTransactionalMap.build(c);
            } else if(_type == StateType.TRANSACTIONAL) {
                mapState = TransactionalMap.build(c);
            } else if(_type == StateType.OPAQUE) {
                mapState = OpaqueMap.build(c);
            } else {
                throw new RuntimeException("Unknown state type: " + _type);
            }
            return new SnapshottableMap(mapState, new Values(_opts.globalKey));
        }


        /**
         * Constructs a Datastore from the dataset ID, using the environment.
         * @param datasetId the GCD dataset ID (same as Google Cloud Project ID)
         * @return the Datastore
         */
        static Datastore makeDatastoreClient(Options opts, String datasetId) throws GeneralSecurityException, IOException {
            return DatastoreFactory.get().create(DatastoreHelper.getOptionsfromEnv().dataset(datasetId).build());
        }
    }

    public GCDState(Datastore datastore, Options opts, Serializer<T> ser) {
        _datastore = datastore;
        _opts = opts;
        _ser = ser;
    }

    public static StateFactory opaque(String datasetId) {
        return opaque(datasetId, new Options<OpaqueValue>());
    }

    public static StateFactory opaque(String datasetId, Options<OpaqueValue> opts) {
        return new Factory(datasetId, StateType.OPAQUE, opts);
    }

    public static StateFactory transactional(String datasetId) {
        return transactional(datasetId, new Options<TransactionalValue>());
    }

    public static StateFactory transactional(String datasetId, Options<TransactionalValue> opts) {
        return new Factory(datasetId, StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional(String datasetId) {
        return nonTransactional(datasetId, new Options<Object>());
    }

    public static StateFactory nonTransactional(String datasetId, Options<Object> opts) {
        return new Factory(datasetId, StateType.NON_TRANSACTIONAL, opts);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> values = new ArrayList(keys.size());
        try {
            for(List<Object> key: keys) {
                T val = null;
                LookupRequest.Builder lreq = LookupRequest.newBuilder();
                Key.Builder dsKey = Key.newBuilder().addPathElement(Key.PathElement.newBuilder().setKind(_opts.datastoreKind).setName(toDatastoreKey(key)));
                lreq.addKey(dsKey);
                LookupResponse lresp = _datastore.lookup(lreq.build());
                if(lresp.getFoundCount() > 0) {
                    Entity entity = lresp.getFound(0).getEntity();
                    for(Property prop : entity.getPropertyList()) {
                        if(prop.getName().equals(_opts.datastoreProperty)) {
                            byte[] bytes = prop.getValue().getBlobValue().toByteArray();
                            val = (T)_ser.deserialize(bytes);
                        }
                    }
                }
                values.add(val);
            }
        } catch(Exception e) {
            throw new ReportedFailedException(e);
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        try {
            for(int i=0; i<keys.size(); i++) {
                String key = toDatastoreKey(keys.get(i));
                T val = vals.get(i);
                byte[] serialized = _ser.serialize(val);
                Key.Builder dsKey = Key.newBuilder().addPathElement(Key.PathElement.newBuilder().setKind(_opts.datastoreKind).setName(key));
                CommitRequest.Builder creq = CommitRequest.newBuilder().setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
                Entity.Builder entity = Entity.newBuilder().setKey(dsKey);
                entity.addProperty(Property.newBuilder().setName(_opts.datastoreProperty).setValue(Value.newBuilder().setBlobValue(ByteString.copyFrom(serialized))));
                creq.setMutation(Mutation.newBuilder().addUpsert(entity));
                _datastore.commit(creq.build());
            }
        } catch(Exception e) {
            throw new ReportedFailedException(e);
        }
    }

    private String toDatastoreKey(List<Object> key) {
        return key.get(0).toString();
    }
}

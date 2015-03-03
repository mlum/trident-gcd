package trident.gcd;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.IBackingMap;

public class GCDState<T> implements IBackingMap<T> {

    public static class Options<T> implements Serializable {
    }

    protected static class Factory implements StateFactory {
        public Factory(String datasetId, StateType type, Options options) {
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return null;
        }
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
    }
}

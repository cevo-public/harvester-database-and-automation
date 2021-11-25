package ch.ethz.harvester.gisaid;


/**
 * This class is used as the return value of functions that might encounter problems with weird entries.
 */
public class MaybeResult<T> {

    /**
     * The analysis result: the value will be used if goodEnough=true.
     */
    private final T result;

    /**
     * Whether the result may be used.
     */
    private final boolean goodEnough;

    /**
     * If it is null, then the entry does not seem to be weird for the function.
     */
    private final WeirdEntryReport weirdEntryReport;

    public MaybeResult(T result) {
        this(result, true, null);
    }

    public MaybeResult(T result, boolean goodEnough, WeirdEntryReport weirdEntryReport) {
        this.result = result;
        this.goodEnough = goodEnough;
        this.weirdEntryReport = weirdEntryReport;
    }

    public T getResult() {
        return result;
    }

    public boolean isGoodEnough() {
        return goodEnough;
    }

    public WeirdEntryReport getWeirdEntryReport() {
        return weirdEntryReport;
    }
}

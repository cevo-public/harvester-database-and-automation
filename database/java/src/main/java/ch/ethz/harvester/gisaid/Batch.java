package ch.ethz.harvester.gisaid;

import java.util.List;


public class Batch {

    private final List<Sequence> sequences;

    public Batch(List<Sequence> sequences) {
        this.sequences = sequences;
    }

    public List<Sequence> getSequences() {
        return sequences;
    }
}

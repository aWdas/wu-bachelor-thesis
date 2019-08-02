package at.hadl.logstatistics.utils.graphbuilding;

public class TriplesElementWalkerFactory {
    private UUIDGenerator uuidGenerator;

    public TriplesElementWalkerFactory(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public TriplesElementWalker createTripleElementWalker() {
        TriplesPathWalker triplesPathWalker = new TriplesPathWalker(uuidGenerator);
        return new TriplesElementWalker(triplesPathWalker);
    }
}

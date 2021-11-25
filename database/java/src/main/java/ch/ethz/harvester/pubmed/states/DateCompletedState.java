package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.StackMachineContext;

import java.time.LocalDate;
import java.util.Optional;


public class DateCompletedState extends StackMachineState {
    private ValueState<Integer> year;
    private ValueState<Integer> month;
    private ValueState<Integer> day;

    @Override
    public String getElementName() {
        return "DateCompleted";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        return Optional.of(switch (elementName) {
            case "Year" -> year = new ValueState<>("Year", Integer::parseInt);
            case "Month" -> month = new ValueState<>("Month", Integer::parseInt);
            case "Day" -> day = new ValueState<>("Day", Integer::parseInt);
            default -> throw new RuntimeException("Unexpected element");
        });
    }

    @Override
    public void entering(StackMachineContext context) {
    }

    @Override
    public void leaving(StackMachineContext context) {
        if (year == null || month == null || day == null) {
            throw new RuntimeException("Date object is not complete.");
        }
        context.getArticles().getFirst().setDateCompleted(
                LocalDate.of(year.getValue(), month.getValue(), day.getValue()));
    }
}

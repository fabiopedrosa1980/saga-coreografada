package br.com.pedrosa.messaging;

import br.com.pedrosa.events.SeatReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static br.com.pedrosa.common.KafkaConfigProperties.SEAT_RESERVED_TOPIC;

@Component
@Slf4j
@RequiredArgsConstructor
public class SeatReserveProducer {

    private final KafkaTemplate<String, SeatReservedEvent> template;

    public void publishSeatReserveEvents(SeatReservedEvent reservedEvent) {
        try {
            log.info("SeatReserveProducer:: Publishing seatReserved event for bookingId {}", reservedEvent.bookingId());
            template.send(SEAT_RESERVED_TOPIC, reservedEvent.bookingId(), reservedEvent);
        } catch (Exception e) {
            log.error("SeatReserveProducer:: Error while publishing seatReserved event for bookingId {}: {}", reservedEvent.bookingId(), e.getMessage());
        }
    }
}

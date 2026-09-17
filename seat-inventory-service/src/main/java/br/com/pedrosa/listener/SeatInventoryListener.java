package br.com.pedrosa.listener;

import br.com.pedrosa.events.BookingCreatedEvent;
import br.com.pedrosa.service.SeatInventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static br.com.pedrosa.common.KafkaConfigProperties.MOVIE_BOOKING_EVENTS_TOPIC;
import static br.com.pedrosa.common.KafkaConfigProperties.SEAT_EVENT_GROUP;

@Component
@Slf4j
@RequiredArgsConstructor
public class SeatInventoryListener {

    private final SeatInventoryService seatInventoryService;

    @KafkaListener(topics = MOVIE_BOOKING_EVENTS_TOPIC, groupId = SEAT_EVENT_GROUP)
    public void consumeBookingEvents(BookingCreatedEvent event) {
        // Logic to update seat inventory based on booking event
        log.info("SeatInventoryListener:: Consuming bookingCreated event for bookingId {}", event.bookingId());
        seatInventoryService.handleBooking(event);
    }
}

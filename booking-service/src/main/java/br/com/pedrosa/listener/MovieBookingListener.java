package br.com.pedrosa.listener;

import br.com.pedrosa.events.SeatReservedEvent;
import br.com.pedrosa.service.BookingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static br.com.pedrosa.common.KafkaConfigProperties.MOVIE_BOOKING_GROUP;
import static br.com.pedrosa.common.KafkaConfigProperties.SEAT_RESERVED_TOPIC;

@Component
@Slf4j
@RequiredArgsConstructor
public class MovieBookingListener {

    private final BookingService service;

    @KafkaListener(topics = SEAT_RESERVED_TOPIC, groupId = MOVIE_BOOKING_GROUP)
    public void consumeSeatReserveEvents(SeatReservedEvent event){

        log.info("MovieBookingListener:: Consuming seatReserved event");

        if(event.reserved()){
            log.info("Booking process completed for bookingId: {}", event.bookingId());
        }else{
            //rollback
            log.info("Seat reservation failed for bookingId: {}", event.bookingId());
            service.handleBookingOnSeatReservationFailure(event.bookingId());
        }

    }
}

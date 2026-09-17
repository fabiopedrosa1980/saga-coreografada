package br.com.pedrosa.service;

import br.com.pedrosa.entity.Booking;
import br.com.pedrosa.enums.BookingStatus;
import br.com.pedrosa.events.BookingCreatedEvent;
import br.com.pedrosa.messaging.BookingEventProducer;
import br.com.pedrosa.repository.BookingRepository;
import br.com.pedrosa.request.BookingRequest;
import br.com.pedrosa.response.BookingResponse;
import br.com.pedrosa.utils.mapper.EntityToBookingResponseMapper;
import br.com.pedrosa.utils.mapper.BookingRequestToEntityMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


@Service
@Slf4j
@RequiredArgsConstructor
public class BookingService {

    private final BookingRepository bookingRepository;

    private final BookingEventProducer bookingEventProducer;

    /**
     * Reserves seats for a given show and user.
     * Validates the request, maps it to an entity, persists it and returns a response DTO.
     *
     * @param request the seat reservation request
     * @return SeatReserveResponse containing reservation details
     * @throws IllegalArgumentException if the request is invalid
     */

    public BookingResponse bookSeats(BookingRequest request) {

        log.info("Booking seats for user {} for show {}", request.userId(), request.showId());

        // Map request -> entity
        var reservationEntity = BookingRequestToEntityMapper.map(request);

        // Persist and map to response
        var savedReservation = bookingRepository.save(reservationEntity);

        // Publish booking created event
        var bookingCreatedEvent = buildBookingCreateEvents(savedReservation);
        bookingEventProducer.publishBookingEvents(bookingCreatedEvent);

        var response = EntityToBookingResponseMapper.map(savedReservation);

        log.info("Seats confirmed with reservation id {}", response.reservationId());
        return response;
    }

    public void confirmeBooking(String bookingId){
        var booking = bookingRepository.findByBookingCode(bookingId);
        booking.setStatus(BookingStatus.CONFIRMED.name());
        bookingRepository.save(booking);
    }

    public BookingResponse findByBookingCode(String bookingId){
        var booking = bookingRepository.findByBookingCode(bookingId);
        return EntityToBookingResponseMapper.map(booking);
    }

    private BookingCreatedEvent buildBookingCreateEvents(Booking savedReservation) {
        return new BookingCreatedEvent(savedReservation.getBookingCode(), savedReservation.getUserId(), savedReservation.getShowId(), savedReservation.getSeatIds(), savedReservation.getAmount());
    }


    public void handleBookingOnSeatReservationFailure(String bookingId) {
        log.info("BookingService:: Handling booking failure for bookingId {}", bookingId);
        var bookingDetails = bookingRepository.findByBookingCode(bookingId);
        if (bookingDetails != null) {
            bookingDetails.setStatus(BookingStatus.FAILED.name());
            bookingRepository.save(bookingDetails);
            log.info("BookingService:: Booking marked as FAILED for bookingId {}", bookingId);
        } else {
            log.warn("BookingService:: No booking found with bookingId {}", bookingId);
        }

    }
}

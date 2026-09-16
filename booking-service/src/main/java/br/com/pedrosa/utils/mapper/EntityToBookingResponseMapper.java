package br.com.pedrosa.utils.mapper;

import br.com.pedrosa.entity.Booking;
import br.com.pedrosa.response.BookingResponse;

public class EntityToBookingResponseMapper {

    public static BookingResponse map(Booking booking) {
        return new BookingResponse(booking.getBookingCode(),
                booking.getStatus());
    }
}

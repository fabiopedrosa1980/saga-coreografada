package br.com.pedrosa.controller;

import br.com.pedrosa.request.BookingRequest;
import br.com.pedrosa.response.BookingResponse;
import br.com.pedrosa.service.BookingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/booking")
@RequiredArgsConstructor
public class BookingController {

    private final BookingService bookingService;

    @PostMapping
    public ResponseEntity<BookingResponse> bookSeat(@RequestBody BookingRequest request) {
        BookingResponse response = bookingService.bookSeats(request);
        return ResponseEntity.ok(response);
    }

    @GetMapping("{reservationId}")
    public ResponseEntity<BookingResponse> getStatus(@PathVariable String reservationId){
        return ResponseEntity.ok(bookingService.findByBookingCode(reservationId));
    }
}

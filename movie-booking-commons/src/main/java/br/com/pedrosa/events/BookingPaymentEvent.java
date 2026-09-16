package br.com.pedrosa.events;

public record BookingPaymentEvent(String bookingId, boolean paymentCompleted, long amount) {
}

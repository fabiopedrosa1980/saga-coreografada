package br.com.pedrosa.events;

public record SeatReservedEvent(String bookingId, boolean reserved, long amount) {}
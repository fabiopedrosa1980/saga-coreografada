package br.com.pedrosa.listener;

import br.com.pedrosa.events.BookingPaymentEvent;
import br.com.pedrosa.service.SeatInventoryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static br.com.pedrosa.common.KafkaConfigProperties.PAYMENT_EVENTS_TOPIC;
import static br.com.pedrosa.common.KafkaConfigProperties.SEAT_EVENT_GROUP;

@Component
@Slf4j
public class PaymentStatusListener {


    private SeatInventoryService service;

    public PaymentStatusListener(SeatInventoryService service) {
        this.service = service;
    }

    @KafkaListener(topics = PAYMENT_EVENTS_TOPIC, groupId = SEAT_EVENT_GROUP)
    public void consumePaymentStatusEvents(BookingPaymentEvent event) {
     log.info("PaymentStatusListener:: Consuming Booking payment status event {}", event.bookingId());


     if(event.paymentCompleted()){
         log.info("Payment status succeeded for bookingId: {}", event.bookingId());
     }else{
         log.info("Payment failed for bookingId: {}, releasing seats.", event.bookingId());
         service.releaseSeatsOnPaymentFailure(event.bookingId());
     }

    }
}

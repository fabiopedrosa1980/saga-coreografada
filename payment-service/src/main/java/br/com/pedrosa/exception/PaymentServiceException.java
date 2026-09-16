package br.com.pedrosa.exception;

public class PaymentServiceException extends RuntimeException{

    public PaymentServiceException(String message) {
        super(message);
    }
}

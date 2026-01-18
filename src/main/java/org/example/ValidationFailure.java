package org.example;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ValidationFailure {
    private Long checkoutId;
    private boolean userValid;
    private boolean shippingInfoValid;
    private boolean productIdValid;
    private boolean paymentIdValid;
}

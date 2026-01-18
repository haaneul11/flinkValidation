package org.example;

import lombok.Data;

@Data
public class CheckoutEvent {
    private Long id;
    private String userId;
    private String shippingMasterId;
    private String productId;
    private Integer quantity;
    private double totalPrice;
    private String paymentId;

    private boolean userValid;  // 존재 값이 있는지
    private boolean shippingInfoValid;  // 너무 길지 않은지
    private boolean paymentValid; // 유효기간이 지나지 않았는지
    private boolean productValid; // 재고가 존재하는지
}

package com.ecommerce.orderservice.service.impl;

import com.ecommerce.commonlib.exception.BusinessException;
import com.ecommerce.orderservice.dto.payment.PaymentDto;
import com.ecommerce.orderservice.entity.Payment;
import com.ecommerce.orderservice.entity.PaymentStatus;
import com.ecommerce.orderservice.helper.PaymentMappingHelper;
import com.ecommerce.orderservice.repository.PaymentRepository;
import com.ecommerce.orderservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;

    @Override
    public PaymentDto save(PaymentDto paymentDto) {
        if (paymentRepository.existsByOrderIdAndIsPayed(paymentDto.getOrderId())) {
            throw BusinessException.conflict("Order has already been paid.");
        }
        Payment payment = PaymentMappingHelper.map(paymentDto);
        payment.setIsPayed(true);
        payment.setPaymentStatus(PaymentStatus.COMPLETED);
        return PaymentMappingHelper.map(paymentRepository.save(payment));
    }

}

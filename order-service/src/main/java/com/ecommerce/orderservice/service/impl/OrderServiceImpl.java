package com.ecommerce.orderservice.service.impl;

import com.ecommerce.commonlib.security.AuthenticationUtils;
import com.ecommerce.orderservice.dto.order.OrderDto;
import com.ecommerce.orderservice.dto.product.ProductDto;
import com.ecommerce.orderservice.dto.promotion.UnusedPromotionIssueResponse;
import com.ecommerce.orderservice.exception.wrapper.OrderNotFoundException;
import com.ecommerce.orderservice.entity.Order;
import com.ecommerce.orderservice.helper.OrderMappingHelper;
import com.ecommerce.orderservice.repository.OrderRepository;
import com.ecommerce.orderservice.security.JwtTokenFilter;
import com.ecommerce.orderservice.service.CallAPI;
import com.ecommerce.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.*;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Service
public class OrderServiceImpl implements OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceImpl.class);
    private static final String ADMIN = "ADMIN";

    private final OrderRepository orderRepository;
    private final ModelMapper modelMapper;
    private final CallAPI callAPI;

    @Override
    public List<OrderDto> findAll() {
        log.info("OrderDto List, service; fetch orders of current user (all for admin)");
        List<Order> orders = isAdmin()
                ? orderRepository.findAll()
                : orderRepository.findByUserId(AuthenticationUtils.requireUserId());
        List<OrderDto> orderDtos = orders.stream()
                .map(OrderMappingHelper::map)
                .toList();
        attachProducts(orderDtos);
        return orderDtos;
    }

    @Override
    public Page<OrderDto> findAll(int page, int size, String sortBy, String sortOrder) {
        log.info("OrderDto List, service; fetch orders of current user (all for admin) with paging and sorting");
        Sort sort = Sort.by(Sort.Direction.fromString(sortOrder), sortBy);
        Pageable pageable = PageRequest.of(page, size, sort);

        Page<Order> orders = isAdmin()
                ? orderRepository.findAll(pageable)
                : orderRepository.findByUserId(AuthenticationUtils.requireUserId(), pageable);
        List<OrderDto> orderDtos = orders.stream()
                .map(OrderMappingHelper::map)
                .toList();
        attachProducts(orderDtos);
        return new PageImpl<>(orderDtos, pageable, orders.getTotalElements());
    }

    @Override
    public OrderDto findById(Integer orderId) {
        log.info("OrderDto, service; fetch order by id");
        OrderDto orderDto = orderRepository.findById(orderId)
                .map(OrderMappingHelper::map)
                .orElseThrow(() -> new OrderNotFoundException(String.format("Order with id: %d not found", orderId)));
        try {
            orderDto.setProductDto(callAPI.receiverProductDto(orderDto.getProductId()));
        } catch (Exception e) {
            log.error("Error fetching product info: {}", e.getMessage());
        }
        return orderDto;
    }

    @Override
    public Boolean existsByOrderId(Integer orderId) {
        return orderRepository.findById(orderId).isPresent();
    }

    @Override
    public OrderDto save(OrderDto orderDto) {
        log.info("OrderDto, service; save order");
        applyCoupon(orderDto);
        Order order = OrderMappingHelper.map(orderDto);
        order.setUserId(AuthenticationUtils.requireUserId());
        return OrderMappingHelper.map(orderRepository.save(order));
    }

    @Override
    public OrderDto update(OrderDto orderDto) {
        log.info("OrderDto, service; update order");
        Order order = OrderMappingHelper.map(orderDto);
        order.setUserId(ownerOf(orderDto.getOrderId()));
        return OrderMappingHelper.map(orderRepository.save(order));
    }

    @Override
    public OrderDto update(Integer orderId, OrderDto orderDto) {
        log.info("OrderDto, service; update order with orderId");
        OrderDto existingOrderDto = findById(orderId);
        modelMapper.map(orderDto, existingOrderDto);
        Order order = OrderMappingHelper.map(existingOrderDto);
        order.setUserId(ownerOf(orderId));
        return OrderMappingHelper.map(orderRepository.save(order));
    }

    @Override
    public void deleteById(Integer orderId) {
        log.info("Void, service; delete order by id");
        orderRepository.deleteById(orderId);
    }

    private static boolean isAdmin() {
        Authentication auth = AuthenticationUtils.getAuthentication();
        return auth != null && auth.getAuthorities().stream()
                .anyMatch(authority -> ADMIN.equals(authority.getAuthority()));
    }

    // The owner is never taken from the request body, so updates keep the stored value.
    private String ownerOf(Integer orderId) {
        if (orderId == null) {
            return null;
        }
        return orderRepository.findById(orderId)
                .map(Order::getUserId)
                .orElse(null);
    }

    // Fetch each product once per request; many orders share the same product.
    private void attachProducts(List<OrderDto> orderDtos) {
        Map<Integer, ProductDto> products = new HashMap<>();
        for (OrderDto orderDto : orderDtos) {
            Integer productId = orderDto.getProductId();
            if (productId == null) {
                continue;
            }
            if (!products.containsKey(productId)) {
                products.put(productId, fetchProduct(productId));
            }
            orderDto.setProductDto(products.get(productId));
        }
    }

    private ProductDto fetchProduct(Integer productId) {
        try {
            return callAPI.receiverProductDto(productId);
        } catch (Exception e) {
            log.error("Error fetching product info: {}", e.getMessage());
            return null;
        }
    }

    private void applyCoupon(OrderDto orderDto) {
        if (orderDto.getPromotionId() == null) {
            orderDto.setDiscountAmount(null);
            return;
        }
        UnusedPromotionIssueResponse issue = callAPI.unusedPromotionIssue(
                orderDto.getPromotionId(), JwtTokenFilter.getTokenFromRequest());
        BigDecimal fee = money(orderDto.getOrderFee());
        BigDecimal discount = couponDiscount(issue, fee);
        BigDecimal storedFee = discount.compareTo(fee) > 0 ? BigDecimal.ZERO : fee.subtract(discount);
        orderDto.setOrderFee(storedFee.doubleValue());
        orderDto.setDiscountAmount(discount.doubleValue());
        orderDto.setPromotionId(issue.promotionId());
    }

    private static BigDecimal couponDiscount(UnusedPromotionIssueResponse issue, BigDecimal fee) {
        if ("PERCENTAGE".equals(issue.discountType()) && issue.discountPercentage() != null) {
            return fee.multiply(BigDecimal.valueOf(issue.discountPercentage()))
                    .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);
        }
        if (issue.discountAmount() == null) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(issue.discountAmount());
    }

    private static BigDecimal money(Double amount) {
        return amount == null ? BigDecimal.ZERO : BigDecimal.valueOf(amount);
    }
}

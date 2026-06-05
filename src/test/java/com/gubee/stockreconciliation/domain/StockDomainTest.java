package com.gubee.stockreconciliation.domain;

import com.gubee.stockreconciliation.domain.model.OrderState;
import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.enums.OrderLifecycleState;
import com.gubee.stockreconciliation.domain.model.exception.InsufficientStockException;
import com.gubee.stockreconciliation.domain.model.vo.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

class StockDomainTest {

    private static final StockKey KEY = StockKey.of("acc-001", "SKU-A");

    private StockEvent adjustEvent(int available) {
        return new StockEvent(EventId.of("evt-adj"), EventType.STOCK_ADJUSTED, Instant.now(),
                AccountId.of("acc-001"), Sku.of("SKU-A"), null, null, Quantity.of(available), "test");
    }

    private StockEvent orderCreatedEvent(int qty) {
        return new StockEvent(EventId.of("evt-created"), EventType.ORDER_CREATED, Instant.now(),
                AccountId.of("acc-001"), Sku.of("SKU-A"), "ML", "ML-1", Quantity.of(qty), null);
    }

    private StockEvent orderCancelledEvent(int qty) {
        return new StockEvent(EventId.of("evt-cancelled"), EventType.ORDER_CANCELLED, Instant.now(),
                AccountId.of("acc-001"), Sku.of("SKU-A"), "ML", "ML-1", Quantity.of(qty), null);
    }

    @Nested
    @DisplayName("Quantity value object")
    class QuantityTests {

        @Test
        void rejectsNegativeAtConstruction() {
            assertThatThrownBy(() -> Quantity.of(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("negative");
        }

        @Test
        void allowsZero() {
            assertThat(Quantity.zero().value()).isEqualTo(0);
        }

        @Test
        void subtractThrowsWhenResultIsNegative() {
            Quantity five = Quantity.of(5);
            assertThatThrownBy(() -> five.subtract(Quantity.of(6)))
                    .isInstanceOf(InsufficientStockException.class);
        }

        @Test
        void subtractToExactZeroAllowed() {
            assertThat(Quantity.of(5).subtract(Quantity.of(5)).value()).isEqualTo(0);
        }

        @Test
        void addWorks() {
            assertThat(Quantity.of(3).add(Quantity.of(4)).value()).isEqualTo(7);
        }
    }

    @Nested
    @DisplayName("Stock aggregate root")
    class StockTests {

        @Test
        void startsAtZero() {
            Stock stock = Stock.create(KEY);
            assertThat(stock.getAvailableQuantity().value()).isEqualTo(0);
        }

        @Test
        void adjustedSetsAbsoluteValue() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(10));
            assertThat(stock.getAvailableQuantity().value()).isEqualTo(10);
        }

        @Test
        void orderCreatedDeductsStock() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(10));
            StockHistory history = stock.apply(orderCreatedEvent(3));

            assertThat(stock.getAvailableQuantity().value()).isEqualTo(7);
            assertThat(history.getDelta()).isEqualTo(-3);
            assertThat(history.getQuantityBefore().value()).isEqualTo(10);
            assertThat(history.getQuantityAfter().value()).isEqualTo(7);
        }

        @Test
        void orderCancelledRestoresStock() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(10));
            stock.apply(orderCreatedEvent(3));
            stock.apply(orderCancelledEvent(3));

            assertThat(stock.getAvailableQuantity().value()).isEqualTo(10);
        }

        @Test
        void orderCreatedFailsWhenInsufficientStock() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(2));

            assertThatThrownBy(() -> stock.apply(orderCreatedEvent(5)))
                    .isInstanceOf(InsufficientStockException.class);
        }

        @Test
        void stockNeverGoesNegative() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(5));
            stock.apply(orderCreatedEvent(5));

            // stock is 0 — one more order must be rejected
            assertThatThrownBy(() -> stock.apply(orderCreatedEvent(1)))
                    .isInstanceOf(InsufficientStockException.class);

            assertThat(stock.getAvailableQuantity().value()).isEqualTo(0);
        }

        @Test
        void adjustIsAbsoluteNotDelta() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(10));
            stock.apply(orderCreatedEvent(3)); // stock = 7
            stock.apply(adjustEvent(20));      // set to 20, not 7+20=27

            assertThat(stock.getAvailableQuantity().value()).isEqualTo(20);
        }

        @Test
        void applyCreatesHistoryWithCorrectStockId() {
            Stock stock = Stock.create(KEY);
            stock.setId(java.util.UUID.fromString("00000000-0000-0000-0000-000000000001"));
            StockHistory history = stock.apply(adjustEvent(5));

            assertThat(history.getStockId()).isEqualTo(stock.getId());
        }

        @Test
        void stockSyncSentDoesNotChangeBalance() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(10));

            StockEvent syncSent = new StockEvent(EventId.of("evt-sync"), EventType.STOCK_SYNC_SENT,
                    Instant.now(), AccountId.of("acc-001"), Sku.of("SKU-A"),
                    "ML", null, Quantity.of(10), null);
            StockHistory history = stock.apply(syncSent);

            assertThat(stock.getAvailableQuantity().value()).isEqualTo(10);
            assertThat(history.getDelta()).isEqualTo(0);
        }

        @Test
        void marketplaceRestoredRestoresStock() {
            Stock stock = Stock.create(KEY);
            stock.apply(adjustEvent(10));
            stock.apply(orderCreatedEvent(3));
            assertThat(stock.getAvailableQuantity().value()).isEqualTo(7);

            StockEvent restored = new StockEvent(EventId.of("evt-restored"), EventType.MARKETPLACE_STOCK_RESTORED,
                    Instant.now(), AccountId.of("acc-001"), Sku.of("SKU-A"), "ML", "ML-1", Quantity.of(3), null);
            stock.apply(restored);

            assertThat(stock.getAvailableQuantity().value()).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("StockKey value object")
    class StockKeyTests {

        @Test
        void rejectsNullAccountId() {
            assertThatThrownBy(() -> new StockKey(null, Sku.of("SKU")))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void rejectsNullSku() {
            assertThatThrownBy(() -> new StockKey(AccountId.of("acc"), null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void equalityByValue() {
            StockKey k1 = StockKey.of("acc", "SKU");
            StockKey k2 = StockKey.of("acc", "SKU");
            assertThat(k1).isEqualTo(k2);
        }
    }

    @Nested
    @DisplayName("Value object validation")
    class VoValidationTests {

        @Test
        void accountId_rejectsNull() {
            assertThatThrownBy(() -> AccountId.of(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void accountId_rejectsBlank() {
            assertThatThrownBy(() -> AccountId.of("   "))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void sku_rejectsNull() {
            assertThatThrownBy(() -> Sku.of(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void sku_rejectsBlank() {
            assertThatThrownBy(() -> Sku.of(""))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void eventId_rejectsNull() {
            assertThatThrownBy(() -> EventId.of(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void eventId_rejectsBlank() {
            assertThatThrownBy(() -> EventId.of(""))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void quantity_isGreaterThanOrEqual_whenGreater() {
            assertThat(Quantity.of(5).isGreaterThanOrEqual(Quantity.of(3))).isTrue();
        }

        @Test
        void quantity_isGreaterThanOrEqual_whenEqual() {
            assertThat(Quantity.of(5).isGreaterThanOrEqual(Quantity.of(5))).isTrue();
        }

        @Test
        void quantity_isGreaterThanOrEqual_whenLess() {
            assertThat(Quantity.of(2).isGreaterThanOrEqual(Quantity.of(5))).isFalse();
        }
    }

    @Nested
    @DisplayName("StockEvent domain")
    class StockEventTests {

        @Test
        void isOrderEvent_trueForOrderCreated() {
            StockEvent e = new StockEvent(EventId.of("e1"), EventType.ORDER_CREATED, Instant.now(),
                    AccountId.of("acc"), Sku.of("SKU"), "ML", "ML-1", Quantity.of(1), null);
            assertThat(e.isOrderEvent()).isTrue();
        }

        @Test
        void isOrderEvent_trueForOrderCancelled() {
            StockEvent e = new StockEvent(EventId.of("e2"), EventType.ORDER_CANCELLED, Instant.now(),
                    AccountId.of("acc"), Sku.of("SKU"), "ML", "ML-1", Quantity.of(1), null);
            assertThat(e.isOrderEvent()).isTrue();
        }

        @Test
        void isOrderEvent_trueForMarketplaceStockRestored() {
            StockEvent e = new StockEvent(EventId.of("e3"), EventType.MARKETPLACE_STOCK_RESTORED, Instant.now(),
                    AccountId.of("acc"), Sku.of("SKU"), "ML", "ML-1", Quantity.of(1), null);
            assertThat(e.isOrderEvent()).isTrue();
        }

        @Test
        void isOrderEvent_falseForStockAdjusted() {
            StockEvent e = new StockEvent(EventId.of("e4"), EventType.STOCK_ADJUSTED, Instant.now(),
                    AccountId.of("acc"), Sku.of("SKU"), null, null, Quantity.of(10), "reason");
            assertThat(e.isOrderEvent()).isFalse();
        }

        @Test
        void isOrderEvent_falseForStockSyncSent() {
            StockEvent e = new StockEvent(EventId.of("e5"), EventType.STOCK_SYNC_SENT, Instant.now(),
                    AccountId.of("acc"), Sku.of("SKU"), "ML", null, Quantity.of(10), null);
            assertThat(e.isOrderEvent()).isFalse();
        }
    }

    @Nested
    @DisplayName("OrderState domain")
    class OrderStateTests {

        private static final AccountId ACC = AccountId.of("acc-001");
        private static final Sku SKU_VAL = Sku.of("SKU-A");

        @Test
        void createPending_isPending() {
            OrderState os = OrderState.createPending("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2), "evt-cancel");
            assertThat(os.isPending()).isTrue();
            assertThat(os.isCreated()).isFalse();
            assertThat(os.isCancelled()).isFalse();
            assertThat(os.isRestored()).isFalse();
        }

        @Test
        void createCreated_isCreated() {
            OrderState os = OrderState.createCreated("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2));
            assertThat(os.isCreated()).isTrue();
            assertThat(os.isPending()).isFalse();
        }

        @Test
        void transitionTo_cancelled_isCancelled() {
            OrderState os = OrderState.createCreated("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2));
            os.transitionTo(OrderLifecycleState.CANCELLED);
            assertThat(os.isCancelled()).isTrue();
            assertThat(os.isCreated()).isFalse();
        }

        @Test
        void transitionTo_restored_isRestored() {
            OrderState os = OrderState.createCreated("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2));
            os.transitionTo(OrderLifecycleState.RESTORED);
            assertThat(os.isRestored()).isTrue();
            assertThat(os.isCreated()).isFalse();
        }

        @Test
        void isAlreadyRestored_trueWhenCancelled() {
            OrderState os = OrderState.createCreated("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2));
            os.transitionTo(OrderLifecycleState.CANCELLED);
            assertThat(os.isAlreadyRestored()).isTrue();
        }

        @Test
        void isAlreadyRestored_trueWhenRestored() {
            OrderState os = OrderState.createCreated("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2));
            os.transitionTo(OrderLifecycleState.RESTORED);
            assertThat(os.isAlreadyRestored()).isTrue();
        }

        @Test
        void isAlreadyRestored_falseWhenCreated() {
            OrderState os = OrderState.createCreated("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2));
            assertThat(os.isAlreadyRestored()).isFalse();
        }

        @Test
        void pendingCancellationEventId_isPreserved() {
            OrderState os = OrderState.createPending("ML", ACC, "ML-1", SKU_VAL, Quantity.of(2), "evt-x");
            assertThat(os.getPendingCancellationEventId()).isEqualTo("evt-x");
        }
    }
}

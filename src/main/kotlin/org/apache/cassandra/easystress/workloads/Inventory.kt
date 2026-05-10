package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.PartitionKeyGenerator
import org.apache.cassandra.easystress.PopulateOption
import org.apache.cassandra.easystress.RequireAccord
import org.apache.cassandra.easystress.StressContext
import org.apache.cassandra.easystress.WorkloadParameter
import org.apache.cassandra.easystress.commands.Run
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicLong

@RequireAccord
class Inventory : IStressWorkload {
    enum class Impl {
        ACCORD,
        LWT_WORKAROUND,
    }

    @WorkloadParameter("Which transaction system to use")
    var impl = Impl.ACCORD

    @WorkloadParameter("Initial inventory_count seeded into each product row during populate")
    var initialStock: Int = 1000

    @WorkloadParameter("A postfix added to the table name")
    var postfix: String = ""

    @WorkloadParameter("Max LWT retry attempts when the IF clause does not apply (LWT_WORKAROUND only)")
    var lwtMaxRetries: Int = 5

    @WorkloadParameter("Number of units to attempt to reserve per checkout (>=1)")
    var unitsPerCheckout: Int = 1

    private fun productsTable() = if (postfix.isEmpty()) "products" else "products_$postfix"

    private fun cartTable() = if (postfix.isEmpty()) "shopping_cart" else "shopping_cart_$postfix"

    private fun itemName(partitionKey: PartitionKey) = "item${partitionKey.id}"

    private fun checkoutUnits(): Int {
        require(unitsPerCheckout >= 1) { "unitsPerCheckout must be >= 1" }
        return unitsPerCheckout
    }

    override fun prepare(session: CqlSession) {
        // Prepared statements live on the runner so we can share the session.
    }

    override fun schema(): List<String> {
        val txnMode = if (impl == Impl.ACCORD) " WITH transactional_mode = 'full'" else ""
        return listOf(
            """
            CREATE TABLE IF NOT EXISTS ${productsTable()} (
                item text PRIMARY KEY,
                inventory_count int
            )$txnMode
            """.trimIndent(),
            """
            CREATE TABLE IF NOT EXISTS ${cartTable()} (
                user_name text,
                item text,
                item_count int,
                PRIMARY KEY (user_name, item)
            )$txnMode
            """.trimIndent(),
        )
    }

    override fun getRunner(context: StressContext): IStressRunner =
        when (impl) {
            Impl.ACCORD -> accordRunner(context)
            Impl.LWT_WORKAROUND -> lwtRunner(context)
        }

    override fun getPopulateOption(args: Run): PopulateOption = PopulateOption.Custom(args.partitionCount, deletes = false)

    override fun getPopulatePartitionKeyGenerator(): Optional<PartitionKeyGenerator> {
        var current = 0L
        return Optional.of(
            PartitionKeyGenerator(
                { max ->
                    val next = current
                    current = (current + 1) % max
                    next
                },
                "",
            ),
        )
    }

    override fun getDefaultReadRate(): Double = 0.05

    private fun accordRunner(context: StressContext): IStressRunner =
        object : IStressRunner {
            val opCounter = AtomicLong(0L)
            val threadId = context.thread

            val populateInsert =
                context.session.prepare(
                    """
                    INSERT INTO ${productsTable()} (item, inventory_count)
                    VALUES (?, ?)
                    """.trimIndent(),
                )

            val units = checkoutUnits()
            val checkout =
                context.session.prepare(
                    """
                    BEGIN TRANSACTION
                        LET p = (SELECT inventory_count FROM ${productsTable()} WHERE item = ?);
                        IF p IS NOT NULL AND p.inventory_count >= $units THEN
                            UPDATE ${productsTable()}
                                SET inventory_count -= $units
                                WHERE item = ?;
                            INSERT INTO ${cartTable()} (user_name, item, item_count)
                                VALUES (?, ?, $units);
                        END IF
                    COMMIT TRANSACTION
                    """.trimIndent(),
                )

            val readBoth =
                context.session.prepare(
                    """
                    BEGIN TRANSACTION
                        LET p = (SELECT inventory_count FROM ${productsTable()} WHERE item = ?);
                        LET c = (SELECT item_count FROM ${cartTable()}
                                    WHERE user_name = ? AND item = ?);
                        SELECT p.inventory_count, c.item_count;
                    COMMIT TRANSACTION
                    """.trimIndent(),
                )

            override fun getNextPopulate(partitionKey: PartitionKey): Operation {
                val item = itemName(partitionKey)
                return Operation.Mutation(
                    populateInsert
                        .bind()
                        .setString(0, item)
                        .setInt(1, initialStock),
                )
            }

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val item = itemName(partitionKey)
                val user = "u_t${threadId}_n${opCounter.incrementAndGet()}"
                return Operation.Mutation(
                    checkout
                        .bind()
                        .setString(0, item) // LET ... WHERE item = ?
                        .setString(1, item) // UPDATE ... WHERE item = ?
                        .setString(2, user) // INSERT ... VALUES (?, ...)
                        .setString(3, item), // INSERT ...          ..., ?, ...)
                )
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val item = itemName(partitionKey)
                val user = "u_t${threadId}_n${opCounter.get()}"
                return Operation.SelectStatement(
                    readBoth
                        .bind()
                        .setString(0, item)
                        .setString(1, user)
                        .setString(2, item),
                )
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation =
                throw UnsupportedOperationException("Inventory workload does not delete")
        }

    private fun lwtRunner(context: StressContext): IStressRunner =
        object : IStressRunner {
            val opCounter = AtomicLong(0L)
            val threadId = context.thread
            val units = checkoutUnits()

            val checkoutApplied = context.metrics.metricRegistry.meter("inventory.checkout.applied")
            val checkoutOutOfStock = context.metrics.metricRegistry.meter("inventory.checkout.out_of_stock")
            val checkoutProductMissing = context.metrics.metricRegistry.meter("inventory.checkout.product_missing")
            val checkoutRetriesExhausted = context.metrics.metricRegistry.meter("inventory.checkout.retries_exhausted")
            val lwtRetries = context.metrics.metricRegistry.meter("inventory.lwt.retries")
            val cartInserted = context.metrics.metricRegistry.meter("inventory.cart.inserted")
            val cartInsertFailures = context.metrics.metricRegistry.meter("inventory.cart.insert_failures")
            val stockCompensated = context.metrics.metricRegistry.meter("inventory.stock.compensated")
            val stockCompensationRetries = context.metrics.metricRegistry.meter("inventory.stock.compensation_retries")
            val stockCompensationFailures = context.metrics.metricRegistry.meter("inventory.stock.compensation_failures")

            val populateInsert =
                context.session.prepare(
                    """
                    INSERT INTO ${productsTable()} (item, inventory_count)
                    VALUES (?, ?)
                    IF NOT EXISTS
                    """.trimIndent(),
                )

            val readStock =
                context.session.prepare(
                    """
                    SELECT inventory_count
                    FROM ${productsTable()}
                    WHERE item = ?
                    """.trimIndent(),
                )

            val reserveStock =
                context.session.prepare(
                    """
                    UPDATE ${productsTable()}
                    SET inventory_count = ?
                    WHERE item = ?
                    IF inventory_count = ?
                    """.trimIndent(),
                )

            val insertCart =
                context.session.prepare(
                    """
                    INSERT INTO ${cartTable()} (user_name, item, item_count)
                    VALUES (?, ?, ?)
                    """.trimIndent(),
                )

            val readCart =
                context.session.prepare(
                    """
                    SELECT item_count
                    FROM ${cartTable()}
                    WHERE user_name = ? AND item = ?
                    """.trimIndent(),
                )

            override fun getNextPopulate(partitionKey: PartitionKey): Operation {
                val item = itemName(partitionKey)
                return Operation.Mutation(
                    populateInsert
                        .bind()
                        .setString(0, item)
                        .setInt(1, initialStock),
                )
            }

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val item = itemName(partitionKey)
                val user = "u_t${threadId}_n${opCounter.incrementAndGet()}"
                return Operation.ClientMutation(
                    execute = {
                        executeCheckout(item, user)
                    },
                )
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val item = itemName(partitionKey)
                val user = "u_t${threadId}_n${opCounter.get()}"
                return Operation.ClientSelect {
                    readProductAndCart(item, user)
                }
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation =
                throw UnsupportedOperationException("Inventory workload does not delete")

            private fun executeCheckout(
                item: String,
                user: String,
            ): CompletionStage<AsyncResultSet> {
                val promise = CompletableFuture<AsyncResultSet>()

                reserveStock(item, attempt = 1).whenComplete { reservation, reserveError ->
                    if (reserveError != null) {
                        promise.completeExceptionally(reserveError)
                        return@whenComplete
                    }

                    if (reservation.status != ReservationStatus.RESERVED) {
                        promise.complete(reservation.result)
                        return@whenComplete
                    }

                    context.session
                        .executeAsync(
                            insertCart
                                .bind()
                                .setString(0, user)
                                .setString(1, item)
                                .setInt(2, units),
                        ).whenComplete { cartResult, cartError ->
                            if (cartError == null) {
                                cartInserted.mark()
                                promise.complete(cartResult)
                                return@whenComplete
                            }

                            cartInsertFailures.mark()
                            compensateStock(item, attempt = 1).whenComplete { _, compensationError ->
                                if (compensationError != null) {
                                    stockCompensationFailures.mark()
                                }
                                promise.completeExceptionally(cartError)
                            }
                        }
                }

                return promise
            }

            private fun reserveStock(
                item: String,
                attempt: Int,
            ): CompletionStage<ReservationResult> {
                val promise = CompletableFuture<ReservationResult>()

                context.session.executeAsync(readStock.bind(item)).whenComplete { result, error ->
                    if (error != null) {
                        promise.completeExceptionally(error)
                        return@whenComplete
                    }

                    val currentStock = result.inventoryCountOrNull()
                    if (currentStock == null) {
                        checkoutProductMissing.mark()
                        promise.complete(ReservationResult(ReservationStatus.PRODUCT_MISSING, result))
                        return@whenComplete
                    }

                    reserveObservedStock(item, currentStock, attempt, result, promise)
                }

                return promise
            }

            private fun reserveObservedStock(
                item: String,
                currentStock: Int,
                attempt: Int,
                lastResult: AsyncResultSet,
                promise: CompletableFuture<ReservationResult>,
            ) {
                if (currentStock < units) {
                    checkoutOutOfStock.mark()
                    promise.complete(ReservationResult(ReservationStatus.OUT_OF_STOCK, lastResult))
                    return
                }

                val newStock = currentStock - units
                context.session
                    .executeAsync(
                        reserveStock
                            .bind()
                            .setInt(0, newStock)
                            .setString(1, item)
                            .setInt(2, currentStock),
                    ).whenComplete { result, error ->
                        if (error != null) {
                            promise.completeExceptionally(error)
                            return@whenComplete
                        }

                        if (result.wasApplied()) {
                            checkoutApplied.mark()
                            promise.complete(ReservationResult(ReservationStatus.RESERVED, result))
                            return@whenComplete
                        }

                        if (attempt >= lwtMaxRetries) {
                            checkoutRetriesExhausted.mark()
                            promise.complete(ReservationResult(ReservationStatus.RETRIES_EXHAUSTED, result))
                            return@whenComplete
                        }

                        lwtRetries.mark()
                        val observedStock = result.inventoryCountOrNull()
                        if (observedStock == null) {
                            reserveStock(item, attempt + 1).whenComplete { retryResult, retryError ->
                                if (retryError == null) {
                                    promise.complete(retryResult)
                                } else {
                                    promise.completeExceptionally(retryError)
                                }
                            }
                        } else {
                            reserveObservedStock(item, observedStock, attempt + 1, result, promise)
                        }
                    }
            }

            private fun compensateStock(
                item: String,
                attempt: Int,
            ): CompletionStage<AsyncResultSet> {
                val promise = CompletableFuture<AsyncResultSet>()

                context.session.executeAsync(readStock.bind(item)).whenComplete { result, error ->
                    if (error != null) {
                        promise.completeExceptionally(error)
                        return@whenComplete
                    }

                    val currentStock = result.inventoryCountOrNull()
                    if (currentStock == null) {
                        promise.complete(result)
                        return@whenComplete
                    }

                    val restoredStock = currentStock + units
                    context.session
                        .executeAsync(
                            reserveStock
                                .bind()
                                .setInt(0, restoredStock)
                                .setString(1, item)
                                .setInt(2, currentStock),
                        ).whenComplete { updateResult, updateError ->
                            if (updateError != null) {
                                promise.completeExceptionally(updateError)
                                return@whenComplete
                            }

                            if (updateResult.wasApplied() || attempt >= lwtMaxRetries) {
                                if (updateResult.wasApplied()) {
                                    stockCompensated.mark()
                                } else {
                                    stockCompensationFailures.mark()
                                }
                                promise.complete(updateResult)
                                return@whenComplete
                            }

                            stockCompensationRetries.mark()
                            compensateStock(item, attempt + 1).whenComplete { retryResult, retryError ->
                                if (retryError == null) {
                                    promise.complete(retryResult)
                                } else {
                                    promise.completeExceptionally(retryError)
                                }
                            }
                        }
                }

                return promise
            }

            private fun readProductAndCart(
                item: String,
                user: String,
            ): CompletionStage<AsyncResultSet> {
                val promise = CompletableFuture<AsyncResultSet>()

                context.session.executeAsync(readStock.bind(item)).whenComplete { _, productError ->
                    if (productError != null) {
                        promise.completeExceptionally(productError)
                        return@whenComplete
                    }

                    context.session
                        .executeAsync(
                            readCart
                                .bind()
                                .setString(0, user)
                                .setString(1, item),
                        ).whenComplete { cartResult, cartError ->
                            if (cartError == null) {
                                promise.complete(cartResult)
                            } else {
                                promise.completeExceptionally(cartError)
                            }
                        }
                }

                return promise
            }

            private fun AsyncResultSet.inventoryCountOrNull(): Int? {
                val row = currentPage().firstOrNull() ?: return null
                return row.getInt("inventory_count")
            }
        }

    private data class ReservationResult(
        val status: ReservationStatus,
        val result: AsyncResultSet,
    )

    private enum class ReservationStatus {
        RESERVED,
        OUT_OF_STOCK,
        PRODUCT_MISSING,
        RETRIES_EXHAUSTED,
    }
}

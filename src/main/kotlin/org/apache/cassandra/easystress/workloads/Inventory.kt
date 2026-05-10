package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.PartitionKeyGenerator
import org.apache.cassandra.easystress.PopulateOption
import org.apache.cassandra.easystress.RequireAccord
import org.apache.cassandra.easystress.StressContext
import org.apache.cassandra.easystress.WorkloadParameter
import org.apache.cassandra.easystress.commands.Run
import java.util.Optional
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

    override fun getPopulateOption(args: Run): PopulateOption =
        PopulateOption.Custom(args.partitionCount, deletes = false)

    override fun getPopulatePartitionKeyGenerator(): Optional<PartitionKeyGenerator> =
        Optional.of(PartitionKeyGenerator.sequence("item"))

    override fun getDefaultReadRate(): Double = 0.05


    private fun accordRunner(context: StressContext): IStressRunner =
        object : IStressRunner {
            val opCounter = AtomicLong(0L)
            val threadId = context.thread

            val populateInsert =
                context.session.prepare(
                    """
                    BEGIN TRANSACTION
                        LET p = (SELECT * FROM ${productsTable()} WHERE item = ?);
                        IF p IS NULL THEN
                            INSERT INTO ${productsTable()} (item, inventory_count)
                                VALUES (?, ?);
                        END IF
                    COMMIT TRANSACTION
                    """.trimIndent(),
                )

            val units = unitsPerCheckout
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
                val item = partitionKey.getText()
                return Operation.Mutation(
                    populateInsert
                        .bind()
                        .setString(0, item)
                        .setString(1, item)
                        .setInt(2, initialStock),
                )
            }

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val item = partitionKey.getText()
                val user = "u_t${threadId}_n${opCounter.incrementAndGet()}"
                return Operation.Mutation(
                    checkout
                        .bind()
                        .setString(0, item)   // LET ... WHERE item = ?
                        .setString(1, item)   // UPDATE ... WHERE item = ?
                        .setString(2, user)   // INSERT ... VALUES (?, ...)
                        .setString(3, item),  // INSERT ...          ..., ?, ...)
                )
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val item = partitionKey.getText()
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
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                TODO("Not yet implemented")
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                TODO("Not yet implemented")
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                TODO("Not yet implemented")
            }

        }
}
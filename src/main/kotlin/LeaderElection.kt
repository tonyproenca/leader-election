import org.apache.zookeeper.*
import org.apache.zookeeper.Watcher.Event.EventType.*
import org.apache.zookeeper.data.Stat
import java.util.*
import java.util.Objects.isNull

class LeaderElection : Watcher {

    private lateinit var zooKeeper: ZooKeeper
    private var currentZNodeName = ""

    companion object {
        const val ZOOKEEPER_ADDRESS = "localhost:2181"
        const val SESSION_TIMEOUT = 3000
        const val ELECTION_NAMESPACE = "/election"
        const val TARGET_ZNODE = "/target_znode"

        @JvmStatic
        fun main(args: Array<String>) {
            val leaderElection = LeaderElection()
            leaderElection.connectToZookeeper()
            leaderElection.volunteerForLeadership()
            leaderElection.reelectLeader()
            leaderElection.watchTargetZnode()
            leaderElection.run()
            leaderElection.close()
            println("Disconnected from Zookeeper, exiting application")
        }
    }

    fun volunteerForLeadership() {
        val zNodePrefix = "$ELECTION_NAMESPACE/c_"
        val zNodeFullPath =
            zooKeeper.create(zNodePrefix, ByteArray(0), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)

        println("znode name $zNodeFullPath")
        this.currentZNodeName = zNodeFullPath.replace("$ELECTION_NAMESPACE/", "")
    }

    fun reelectLeader() {
        var predecessorStat: Stat? = null
        var predecessorZnodeName = ""

        while (isNull(predecessorStat)) {
            val children = zooKeeper.getChildren(ELECTION_NAMESPACE, false)
            val smallestChild = children.sorted()[0]
            if (smallestChild == currentZNodeName) {
                println("I am the leader")
                return
            } else {
                println("I am not the leader")
                val predecessorIndex = Collections.binarySearch(children, currentZNodeName) - 1
                predecessorZnodeName = children[predecessorIndex]
                predecessorStat = zooKeeper.exists("$ELECTION_NAMESPACE/$predecessorZnodeName", this)
            }
        }

        println("Watching Znode $predecessorZnodeName")
    }

    private fun connectToZookeeper() {
        zooKeeper = ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this)
    }

    fun run() {
        synchronized(zooKeeper) {
            (zooKeeper as Object).wait();
        }
    }

    fun close() {
        zooKeeper.close()
    }

    fun watchTargetZnode() {
        val stat: Stat? = zooKeeper.exists(TARGET_ZNODE, this)
        if (isNull(stat)) {
            return;
        }

        val data: ByteArray = zooKeeper.getData(TARGET_ZNODE, this, stat)
        val children: MutableList<String> = zooKeeper.getChildren(TARGET_ZNODE, this)

        print("Data: $data children: $children")
    }

    override fun process(event: WatchedEvent) {
        when (event.type) {
            None -> handleNoneState(event)
            NodeCreated -> println("$TARGET_ZNODE was created")
            NodeDeleted -> reelectLeader()
            NodeDataChanged -> println("$TARGET_ZNODE data changed")
            NodeChildrenChanged -> println("$TARGET_ZNODE children changed")
            DataWatchRemoved -> TODO()
            ChildWatchRemoved -> TODO()
            PersistentWatchRemoved -> TODO()
        }
        watchTargetZnode()
    }

    private fun handleNoneState(event: WatchedEvent) {
        if (event.state == Watcher.Event.KeeperState.SyncConnected) {
            println("Successfully connected to Zookeeper")
        } else {
            synchronized(zooKeeper) {
                println("Disconnected from Zookeeper event ")
                (zooKeeper as Object).notifyAll()
            }
        }
    }


}
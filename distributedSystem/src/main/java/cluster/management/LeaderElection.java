package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final  String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;


    public static void main (String [] args ) throws IOException, InterruptedException, KeeperException{
        LeaderElection leaderElection = new LeaderElection();

        leaderElection.connectToZookeeper();
        leaderElection.run();
        leaderElection.close();
        leaderElection.volunteerForLeadership();
        leaderElection.reelecLeader();
        System.out.println("Disconnected from zookeeper, exiting application");
    }

    public void connectToZookeeper () throws IOException{
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);

    }

    public void run () throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close () throws InterruptedException{
        synchronized (zooKeeper){
            zooKeeper.close();
        }
    }

    public void volunteerForLeadership () throws KeeperException, InterruptedException{
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name" + znodeFullPath);

        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }


    public void reelecLeader () throws  KeeperException, InterruptedException{
        Stat predecssorStat = null;
        String predecssorZnodeName = "";

        while (predecssorStat == null){
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)){
                System.out.println("I am the leader");
                return ;
            }else{
                System.out.println(" I am not leader");
                int predecssorIndex = Collections.binarySearch(children, currentZnodeName) -1;
                predecssorZnodeName = children.get(predecssorIndex);
                predecssorStat = zooKeeper.exists(ELECTION_NAMESPACE + '/' + predecssorZnodeName, this);

            }

        }
        System.out.println("Watching znode" + predecssorZnodeName);
        System.out.println();



    }



    @Override
    public void process(WatchedEvent event){
        switch (event.getType()) {
            case None:
                if(event.getState() == Watcher.Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to ZooKeeper");
                }else {
                    synchronized (zooKeeper){
                        System.out.println("Disconnect from zookeeper");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                try{
                    reelecLeader();
                }catch(KeeperException e){

            }catch(InterruptedException e){

            }
        }
    }

}

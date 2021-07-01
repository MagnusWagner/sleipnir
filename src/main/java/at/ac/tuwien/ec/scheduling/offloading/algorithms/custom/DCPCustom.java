package at.ac.tuwien.ec.scheduling.offloading.algorithms.custom;


import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.offloading.algorithms.heftbased.utils.NodeRankComparator;
import at.ac.tuwien.ec.scheduling.utils.RuntimeComparator;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.jgrapht.graph.DirectedAcyclicGraph;
import scala.Tuple2;
import scala.reflect.macros.Infrastructure;


import java.util.*;

/**
 * OffloadScheduler class that implements the
 * Dynamically Critical Path (DCP) algorithm
 * , a dynamic scheduling heuristic, for efficient application scheduling
 */

public class DCPCustom extends OffloadScheduler {
    /**
     *
     * @param A MobileApplication property from  SimIteration
     * @param I MobileCloudInfrastructure property from  SimIteration
     * Constructors set the parameters and calls setRank() to nodes' ranks
     */
	enum NodeTypus {Local, Edge, Cluster}

    private class SchedulingCluster {
    	private String name;
    	private NodeTypus nodeType;
    	// Schedule with the variables {Start: (MSC, Length)}
    	private HashMap<Double,Tuple2<MobileSoftwareComponent,Double>> schedule;

    	public SchedulingCluster(String name, NodeTypus nodeType) {
    		this.name = name;
    		this.nodeType = nodeType;
		}

    	public String getName() {
    		return this.name;
		}
		public NodeTypus getNodeType() {
			return this.nodeType;
		}


		// Remove a Task if it is in the schedule
    	public void removeTask(MobileSoftwareComponent task) {
    		for (double key: this.schedule.keySet()) {
    			if (this.schedule.get(key).equals(task)) {
					this.schedule.remove(key);
				}
			}
		}

		// Deploy tasks from the schedule to the infrastructure.
		public void deployTasks(MobileCloudInfrastructure infrastructure, String userID, OffloadScheduling scheduling) {
    		// Max_value for minimal runtime calculation over all nodes of selected node type.
    		double tMin = Double.MAX_VALUE;
    		double tCurrent;
    		ComputationalNode localNode = (ComputationalNode) infrastructure.getNodeById(userID);
    		// adding all tasks from the dictionary to a list to make scheduling simpler.
    		ArrayList<MobileSoftwareComponent> orderedTaskList = new ArrayList<MobileSoftwareComponent>();
			for (Double key: this.schedule.keySet()) {
				orderedTaskList.add(this.schedule.get(key)._1);
			}
			ComputationalNode target;
			double node_runtime;
			//
			// Local
			//
			if (this.nodeType == NodeTypus.Local) {
				target = localNode;
				for (MobileSoftwareComponent currTask: orderedTaskList) {
					if (isValid(scheduling,currTask,target)) {
						deploy(scheduling,currTask,target);
					}
				}
			}
			//
			// Edge
			//
			else if (this.nodeType == NodeTypus.Edge) {
				for(ComputationalNode cn : infrastructure.getEdgeNodes().values()) {
					tCurrent = 0.0;
					for (MobileSoftwareComponent currTask: orderedTaskList) {
						// questionable if isValid works like this.
						if (!isValid(scheduling,currTask,target)) tCurrent += Double.MAX_VALUE;
						else tCurrent += currTask.getRuntimeOnNode(localNode, cn, infrastructure);
					}
					if (tMin>tCurrent) {
						tMin = tCurrent;
						target = cn;
					}
				}
				for (MobileSoftwareComponent currTask: orderedTaskList) {
					deploy(scheduling,currTask,target);
				}

			} else {
				// Cloud
				for(ComputationalNode cn : infrastructure.getCloudNodes().values()) {

				}
			}

			for(ComputationalNode cn : infrastructure.getAllNodes())

				node_runtime =


				}
				if(currTask.getRuntimeOnNode(cn, currentInfrastructure) < tMin &&
						isValid(scheduling,currTask,cn))
				{
					tMin = currTask.getRuntimeOnNode(cn, currentInfrastructure); // Earliest Finish Time  EFT = wij + EST
					target = cn;

				}
		}

		public void addTask(MobileSoftwareComponent task, double time, double length) {
			Tuple2<MobileSoftwareComponent,Double> tmp_tuple = new Tuple2<>(task,length);
			this.schedule.put(time, tmp_tuple);
		}

		public List<MobileSoftwareComponent> getScheduledTasks() {
    		return this.schedule.keySet();
		}

    	public double find_slot(MobileSoftwareComponent task, boolean PUSH) {
    			// TODO

		}
	}




	private Map<MobileSoftwareComponent,Tuple2<String,Double>> task_scheduling;
	private Map<String,List<Tuple2<Double,MobileSoftwareComponent>>> cluster_scheduling;
    private Map<MobileSoftwareComponent,Double> AEST_levels;
	private Map<MobileSoftwareComponent,Double> ALST_levels;
	private Map<MobileSoftwareComponent,Double> Mobility_levels;
	private Map<Tuple2<MobileSoftwareComponent,MobileSoftwareComponent>,Double> C_dictionary;
	private Map<Tuple2<MobileSoftwareComponent,NodeTypus>,Double> R_dictionary;
	private Double current_cp;

	public DCPCustom(MobileApplication A, MobileCloudInfrastructure I) {
		super();
		setMobileApplication(A);
		setInfrastructure(I);
		setRank(this.currentApp,this.currentInfrastructure);
	}

	public DCPCustom(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
		super();
		setMobileApplication(t._1());
		setInfrastructure(t._2());
		setRank(this.currentApp,this.currentInfrastructure);
	}

    /**
     * Processor selection phase:
     * select the tasks in order of their priorities and schedule them on its "best" processor,
     * which minimizes task's finish time
     * @return
     */
	@Override
	public ArrayList<? extends OffloadScheduling> findScheduling() {
		double start = System.nanoTime();

		AEST_levels = AEST_calculation(this.currentApp, this.currentInfrastructure);
		ALST_levels = ALST_calculation(this.currentApp, this.currentInfrastructure);
		/*scheduledNodes contains the nodes that have been scheduled for execution.
		 * Once nodes are scheduled, they are taken from the PriorityQueue according to their runtime
		 */
		PriorityQueue<MobileSoftwareComponent> scheduledNodes
				= new PriorityQueue<MobileSoftwareComponent>(new RuntimeComparator());
		/*
		 * tasks contains tasks that have to be scheduled for execution.
		 * Tasks are selected according to their upRank (at least in HEFT)
		 */
		PriorityQueue<MobileSoftwareComponent> tasks = new PriorityQueue<MobileSoftwareComponent>(new NodeRankComparator());

		// To start, we calculate t-Levels of all nodes.
		for (currTask : currentApp.getTaskDependencies().iterator()) {

		};
		tasks.addAll(currentApp.getTaskDependencies().vertexSet());
		ArrayList<OffloadScheduling> deployments = new ArrayList<OffloadScheduling>();

		MobileSoftwareComponent currTask;

		setNodeMobility(,); // Part 1 of the algorithm.

		// UP HERE IS OWN STUFF.
		// TODO
		// TODO
		//
		//




		//We initialize a new OffloadScheduling object, modelling the scheduling computer with this algorithm
		OffloadScheduling scheduling = new OffloadScheduling(); 
		//We check until there are nodes available for scheduling
		while((currTask = tasks.poll())!=null)
		{
			//If there are nodes to be scheduled, we check the first task who terminates and free its resources
			if(!scheduledNodes.isEmpty())
			{
				MobileSoftwareComponent firstTaskToTerminate = scheduledNodes.remove();
				((ComputationalNode) scheduling.get(firstTaskToTerminate)).undeploy(firstTaskToTerminate);
			}
			double tMin = Double.MAX_VALUE; //Minimum execution time for next task
			ComputationalNode target = null;
			if(!currTask.isOffloadable())
			{
			    // If task is not offloadable, deploy it in the mobile device (if enough resources are available)
                if(isValid(scheduling,currTask,(ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId())))
                	target = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId()); 
				
			}
			else
			{	
				//Check for all available Cloud/Edge nodes
				for(ComputationalNode cn : currentInfrastructure.getAllNodes())
					if(currTask.getRuntimeOnNode(cn, currentInfrastructure) < tMin &&
							isValid(scheduling,currTask,cn))
					{
						tMin = currTask.getRuntimeOnNode(cn, currentInfrastructure); // Earliest Finish Time  EFT = wij + EST
						target = cn;
						
					}
				/*
				 * We need this check, because there are cases where, even if the task is offloadable, 
				 * local execution is the best option
				 */
				ComputationalNode localDevice = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
				if(currTask.getRuntimeOnNode(localDevice, currentInfrastructure) < tMin &&
						isValid(scheduling,currTask,localDevice))
				{
					tMin = currTask.getRuntimeOnNode(localDevice, currentInfrastructure); // Earliest Finish Time  EFT = wij + EST
					target = localDevice;
				}
			}
			//if scheduling found a target node for the task, it allocates it to the target node
			if(target != null)
			{
				deploy(scheduling,currTask,target);
				scheduledNodes.add(currTask);
			}
			/*
			 * if simulation considers mobility, perform post-scheduling operations
			 * (default is to update coordinates of mobile devices)
			 */
			if(OffloadingSetup.mobility)
				postTaskScheduling(scheduling);					
		}
		double end = System.nanoTime();
		scheduling.setExecutionTime(end-start);
		deployments.add(scheduling);
		return deployments;
	}

	// to be changed TODO
	protected void setRank(MobileApplication A, MobileCloudInfrastructure I)
	{
		for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
			msc.setVisited(false);

		for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
			upRank(msc,A.getTaskDependencies(),I);

	}




    private HashMap<MobileSoftwareComponent,Double> AEST_calculation(MobileApplication A, MobileCloudInfrastructure I) {
		AEST_levels = new HashMap<MobileSoftwareComponent,Double>();
		List<MobileSoftwareComponent> taskList = new ArrayList<MobileSoftwareComponent>();
		DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag = A.getTaskDependencies();
		for (MobileSoftwareComponent mobileSoftwareComponent : dag) taskList.add(mobileSoftwareComponent);
		// Collections.reverse(taskList);
		for (MobileSoftwareComponent task : taskList) {
			AEST_levels.put(task,AEST(task, dag, I));
		};
	};


	private double AEST(MobileSoftwareComponent task, DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag, MobileCloudInfrastructure I) {
		if (AEST_levels.containsKey(task)) return AEST_levels.get(task);
		else {
			if (dag.outgoingEdgesOf(task).isEmpty()) return 0.0; // if it is an entry node
			double max = 0.0;
			// for each parent
			Set<MobileSoftwareComponent> parents = dag.getAncestors(task);
			for (MobileSoftwareComponent n_x : parents) {
				// this should be the check on if the task is already scheduled on some node.
				if (n_x.isVisited()) {

				}
			}
			// TODO
			double w_cmp = 0.0; // average execution time of task on each processor / node of this component
			int numberOfNodes = I.getAllNodes().size() + 1;
			for(ComputationalNode cn : I.getAllNodes())
				w_cmp += task.getLocalRuntimeOnNode(cn, I);
			w_cmp = w_cmp / numberOfNodes;

			// rank is equivalent to b level
			double max = 0;
			for(ComponentLink neigh : dag.outgoingEdgesOf(task)){

				MobileSoftwareComponent ny = neigh.getTarget();
				double neigh_b_level = b_level(ny,dag,I);

				if (ny.getRank() > max) {
					max = ny.getRank();
				}
			}
			task.setRank(w_cmp + max);
		}
	};

	private double AEST(MobileSoftwareComponent task, DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag, MobileCloudInfrastructure I) {
		if (b_levels.containsKey(task)) return b_levels.get(task);
		else {
			double w_cmp = 0.0; // average execution time of task on each processor / node of this component
			int numberOfNodes = I.getAllNodes().size() + 1;
			for(ComputationalNode cn : I.getAllNodes())
				w_cmp += task.getLocalRuntimeOnNode(cn, I);
			w_cmp = w_cmp / numberOfNodes;

			// rank is equivalent to b level
			double max = 0;
			for(ComponentLink neigh : dag.outgoingEdgesOf(task)){

				MobileSoftwareComponent ny = neigh.getTarget();
				double neigh_b_level = b_level(ny,dag,I);

				if (ny.getRank() > max) {
					max = ny.getRank();
				}
			}
			task.setRank(w_cmp + max);
		}
	};



	private void bLevel(MobileSoftwareComponent msc, DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag,
						MobileCloudInfrastructure infrastructure) {


		double w_cmp = 0.0; // average execution time of task on each processor / node of this component
		int numberOfNodes = infrastructure.getAllNodes().size() + 1;
		for(ComputationalNode cn : infrastructure.getAllNodes())
			w_cmp += msc.getLocalRuntimeOnNode(cn, infrastructure);

		w_cmp = w_cmp / numberOfNodes;

		// rank is equivalent to b level
		double max = 0;
		for(ComponentLink neigh : dag.outgoingEdgesOf(msc)){

			MobileSoftwareComponent ny = neigh.getTarget();

			if (ny.getRank() > max) {
				max = ny.getRank();
			}
		}
		msc.setRank(w_cmp + max);
	}



	/**
	 * upRank is the task prioritizing phase of DCP
	 * rank is computed recuversively by traversing the task graph upward
	 * @param msc Part of the software that needs to be processed
	 * @param dag Mobile Application's DAG
	 * @param infrastructure
	 * @return the upward rank of msc
	 * (which is also the length of the critical path (CP) of the whole DAG - (b-level of msc + t-level of msc))
	 */
	private double upRank(MobileSoftwareComponent msc, DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag,
			MobileCloudInfrastructure infrastructure) {
		// TODO: Calculate mobility (CP is maximum b-level of root nodes)
		double w_cmp = 0.0; // average execution time of task on each processor / node of this component
		if(!msc.isVisited())
        /*  since upward Rank is defined recursively, visited makes sure no extra unnecessary computations are done when
		    calling upRank on all nodes during initialization */
        {
			msc.setVisited(true);
			int numberOfNodes = infrastructure.getAllNodes().size() + 1;
			for(ComputationalNode cn : infrastructure.getAllNodes())
				w_cmp += msc.getLocalRuntimeOnNode(cn, infrastructure);
			
			w_cmp = w_cmp / numberOfNodes;

            double tmpWRank;
            double maxSRank = 0; // max successor rank
            for(ComponentLink neigh : dag.outgoingEdgesOf(msc)) // for the exit task rank=w_cmp
            {
                // rank = w_Cmp +  max(cij + rank(j)    for all j in succ(i)
                // where cij is the average commmunication cost of edge (i, j)
                tmpWRank = upRank(neigh.getTarget(),dag,infrastructure); // succesor's rank
                double tmpCRank = 0;  // this component's average Communication rank
                //We consider only offloadable successors. If a successor is not offloadable, communication cost is 0
                if (neigh.getTarget().isOffloadable())
                {
                    for(ComputationalNode cn : infrastructure.getAllNodes())
                        tmpCRank += infrastructure.getTransmissionTime(neigh.getTarget(), infrastructure.getNodeById(msc.getUserId()), cn);
                    tmpCRank = tmpCRank / (infrastructure.getAllNodes().size());
                }
                double tmpRank = tmpWRank + tmpCRank;
                maxSRank = (tmpRank > maxSRank)? tmpRank : maxSRank;
            }
            msc.setRank(w_cmp + maxSRank);
		}
		return msc.getRank();
	}
	
}

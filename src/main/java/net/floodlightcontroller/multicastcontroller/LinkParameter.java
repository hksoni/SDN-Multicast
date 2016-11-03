package net.floodlightcontroller.multicastcontroller;

public class LinkParameter {
	long capacity;
	long utilization;
	double load;
	public LinkParameter(long capacity, long utilization) {
		super();
		this.capacity = capacity;
		this.utilization = utilization;
		this.load = ((double)utilization/(double)capacity)*100;
	}
	
	public double addUtilization(long util) {
		this.utilization += util;
		this.load = (((double)utilization/(double)capacity))*100;
		return (((double)util/(double)capacity))*100;
	}
	
	public double removeUtilization(long util) {
		this.utilization -= util;
		this.load = (((double)utilization/(double)capacity))*100;
		return (((double)util/(double)capacity))*100;
	}
	
	@Override
	public String toString() {
		return ("utilization="+Long.toString(utilization)+
				",capacity="+Long.toString(capacity)+
				",load-percent="+Double.toString(load)
				);
	}
	
};

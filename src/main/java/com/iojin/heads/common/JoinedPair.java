package com.iojin.heads.common;

public class JoinedPair implements Comparable<JoinedPair> {
    private long rid;
    private long sid;
    private double dist;

    public JoinedPair(long rid, long sid, double dist) {
        this.sid = sid;
        this.dist = dist;
        this.rid = rid;
    }

    public JoinedPair(String string) {
        double[] values = FormatUtils.toDoubleArray(string);
        this.rid = (long) values[0];
        this.sid = (long) values[1];
        this.dist = values[2];
    }



    public long getRid() {
        return rid;
    }

    public void setRid(long rid) {
        this.rid = rid;
    }

    public long getSid() {
        return sid;
    }
    public void setSid(long sid) {
        this.sid = sid;
    }
    public double getDist() {
        return dist;
    }
    public void setDist(double dist) {
        this.dist = dist;
    }

    @Override
    public int compareTo(JoinedPair o) {
        if (Math.abs(this.dist - o.dist) < DistanceUtils.EPSILON) {
            if ((this.rid == o.sid && this.sid == o.rid) || (this.sid == o.sid && this.rid == o.rid)) {
                return 0;
            }
            else if (this.rid != o.rid) {
                return (int)(this.rid - o.rid);
            }
            else {
                return (int)(this.sid - o.sid);
            }
        }
        else return (this.dist - o.dist) > 0 ? 1 : -1;
    }

    @Override
    public int hashCode() {
        return (int) (this.getRid() + this.getSid());
    }

    @Override
    public boolean equals(Object other) {
        JoinedPair pair = (JoinedPair) other;
        return (this.rid == pair.getRid() && this.sid == pair.getSid() || this.sid  == pair.getRid() && this.rid == pair.getSid());
    }

    @Override
    public String toString() {
        return rid + " " + sid + " " + dist;
    }

}

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by George on 2017/6/22.
 */
public class TwoSortKey implements Serializable,Ordered<TwoSortKey> {//二次排序key
    private int first;
    private int second;
    public TwoSortKey(int first,int second){
        super();
        this.first=first;
        this.second=second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compare(TwoSortKey that) {
        if(this.first==that.getFirst()){
            return (this.second-that.getSecond());
        }
        else{
            return (this.first-that.getFirst());
        }
    }

    @Override
    public boolean $less(TwoSortKey that) {
        if(this.first<that.getFirst()){
            return true;
        }
        if(this.first==that.getFirst()&&this.second<that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(TwoSortKey that) {
        if(this.first>that.getFirst()){
            return true;
        }
        if(this.first==that.getFirst()&&this.second>that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(TwoSortKey that) {
       if($less(that)){
           return true;
       }
       if(this.first==that.getFirst()&&this.second==that.getSecond()){
           return true;
       }
       return false;
    }

    @Override
    public boolean $greater$eq(TwoSortKey that) {
        if($greater(that)){
            return true;
        }
        if(this.first==that.getFirst()&&this.second==that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(TwoSortKey that) {
        if(this.first==that.getFirst()){
            return (this.second-that.getSecond());
        }
        else{
            return (this.first-that.getFirst());
        }
    }
}

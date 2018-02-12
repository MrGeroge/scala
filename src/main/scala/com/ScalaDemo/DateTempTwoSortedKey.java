package com.ScalaDemo;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by George on 2017/11/22.
 */
public class DateTempTwoSortedKey implements Serializable,Ordered<DateTempTwoSortedKey> { //组合键，定义顺序比较策略
    private String yearMonth;
    private int temperature;

    public DateTempTwoSortedKey(String yearMonth, int temperature) {
        this.yearMonth = yearMonth;
        this.temperature = temperature;
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public int compare(DateTempTwoSortedKey that) {
        int compareValue=this.yearMonth.compareTo(that.getYearMonth());
        if(compareValue!=0){
            return compareValue;
        }
        else{
            compareValue=this.temperature-that.getTemperature();
            return compareValue;
        }
    }

    @Override
    public boolean $less(DateTempTwoSortedKey that) {
        if(this.yearMonth.compareTo(that.getYearMonth())<0){
            return true;
        }
        else if(this.yearMonth.compareTo(that.getYearMonth())==0){
            if(this.temperature-that.getTemperature()<0){
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean $greater(DateTempTwoSortedKey that) {
        if(this.yearMonth.compareTo(that.getYearMonth())>0){
            return true;
        }
        else if(this.yearMonth.compareTo(that.getYearMonth())==0){
            if(this.temperature-that.getTemperature()>0){
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean $less$eq(DateTempTwoSortedKey that) {
       if($less(that)){
           return true;
       }
       else if(compare(that)==0){
           return true;
       }
       else{
           return false;
       }
    }

    @Override
    public boolean $greater$eq(DateTempTwoSortedKey that) {
        if($greater(that)){
            return true;
        }
        else if(compare(that)==0){
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public int compareTo(DateTempTwoSortedKey that) {
        int compareValue=this.yearMonth.compareTo(that.getYearMonth());
        if(compareValue!=0){
            return compareValue;
        }
        else{
            compareValue=this.temperature-that.getTemperature();
            return compareValue;
        }
    }
}

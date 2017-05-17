package es.us.lsi.hermes.smartDriver;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DataSection implements Serializable {

    @SerializedName("medianSpeed")
    @Expose
    private Double medianSpeed;
    @SerializedName("roadSection")
    @Expose
    private List<RoadSection> roadSection = new ArrayList<RoadSection>();
    @SerializedName("standardDeviationSpeed")
    @Expose
    private Double standardDeviationSpeed;
    @SerializedName("averageRR")
    @Expose
    private Double averageRR;
    @SerializedName("averageHeartRate")
    @Expose
    private Double averageHeartRate;
    @SerializedName("standardDeviationRR")
    @Expose
    private Double standardDeviationRR;
    @SerializedName("averageDeceleration")
    @Expose
    private Double averageDeceleration;
    @SerializedName("numHighDecelerations")
    @Expose
    private Integer numHighDecelerations;
    @SerializedName("averageAcceleration")
    @Expose
    private Double averageAcceleration;
    @SerializedName("maxSpeed")
    @Expose
    private Double maxSpeed;
    @SerializedName("rrSection")
    @Expose
    private List<Integer> rrSection = new ArrayList<Integer>();
    @SerializedName("numHighAccelerations")
    @Expose
    private Integer numHighAccelerations;
    @SerializedName("pke")
    @Expose
    private Double pke;
    @SerializedName("standardDeviationHeartRate")
    @Expose
    private Double standardDeviationHeartRate;
    @SerializedName("averageSpeed")
    @Expose
    private Double averageSpeed;
    @SerializedName("minSpeed")
    @Expose
    private Double minSpeed;

    public Double getMedianSpeed() {
        return medianSpeed;
    }

    public void setMedianSpeed(Double medianSpeed) {
        this.medianSpeed = medianSpeed;
    }

    public List<RoadSection> getRoadSection() {
        return roadSection;
    }

    public void setRoadSection(List<RoadSection> roadSection) {
        this.roadSection = roadSection;
    }

    public Double getStandardDeviationSpeed() {
        return standardDeviationSpeed;
    }

    public void setStandardDeviationSpeed(Double standardDeviationSpeed) {
        this.standardDeviationSpeed = standardDeviationSpeed;
    }

    public Double getAverageRR() {
        return averageRR;
    }

    public void setAverageRR(Double averageRR) {
        this.averageRR = averageRR;
    }

    public Double getAverageHeartRate() {
        return averageHeartRate;
    }

    public void setAverageHeartRate(Double averageHeartRate) {
        this.averageHeartRate = averageHeartRate;
    }

    public Double getStandardDeviationRR() {
        return standardDeviationRR;
    }

    public void setStandardDeviationRR(Double standardDeviationRR) {
        this.standardDeviationRR = standardDeviationRR;
    }

    public Double getAverageDeceleration() {
        return averageDeceleration;
    }

    public void setAverageDeceleration(Double averageDeceleration) {
        this.averageDeceleration = averageDeceleration;
    }

    public Integer getNumHighDecelerations() {
        return numHighDecelerations;
    }

    public void setNumHighDecelerations(Integer numHighDecelerations) {
        this.numHighDecelerations = numHighDecelerations;
    }

    public Double getAverageAcceleration() {
        return averageAcceleration;
    }

    public void setAverageAcceleration(Double averageAcceleration) {
        this.averageAcceleration = averageAcceleration;
    }

    public Double getMaxSpeed() {
        return maxSpeed;
    }

    public void setMaxSpeed(Double maxSpeed) {
        this.maxSpeed = maxSpeed;
    }

    public List<Integer> getRrSection() {
        return rrSection;
    }

    public void setRrSection(List<Integer> rrSection) {
        this.rrSection = rrSection;
    }

    public Integer getNumHighAccelerations() {
        return numHighAccelerations;
    }

    public void setNumHighAccelerations(Integer numHighAccelerations) {
        this.numHighAccelerations = numHighAccelerations;
    }

    public Double getPke() {
        return pke;
    }

    public void setPke(Double pke) {
        this.pke = pke;
    }

    public Double getStandardDeviationHeartRate() {
        return standardDeviationHeartRate;
    }

    public void setStandardDeviationHeartRate(Double standardDeviationHeartRate) {
        this.standardDeviationHeartRate = standardDeviationHeartRate;
    }

    public Double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(Double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public Double getMinSpeed() {
        return minSpeed;
    }

    public void setMinSpeed(Double minSpeed) {
        this.minSpeed = minSpeed;
    }

}

package ch.ethz.harvester.viollier;

import java.time.LocalDate;

class Sample {

    private int sampleNumber;
    private String canton;
    private String city;
    private String zipCode;
    private String sequencingCenter;
    private LocalDate orderDate;
    private Integer ct;
    private String viollierPlateName;
    private String wellPosition;

    public int getSampleNumber() {
        return sampleNumber;
    }
    public int getTestID() {
        return sequencingCenter + "/" + sampleNumber;
    }

    public Sample setSampleNumber(int sampleNumber) {
        this.sampleNumber = sampleNumber;
        return this;
    }

    public String getCanton() {
        return canton;
    }

    public Sample setCanton(String canton) {
        this.canton = canton;
        return this;
    }

    public String getCity() {
        return city;
    }

    public Sample setCity(String city) {
        this.city = city;
        return this;
    }

    public String getZipCode() {
        return zipCode;
    }

    public Sample setZipCode(String zipCode) {
        this.zipCode = zipCode;
        return this;
    }

    public String getSequencingCenter() {
        return sequencingCenter;
    }

    public Sample setSequencingCenter(String sequencingCenter) {
        this.sequencingCenter = sequencingCenter;
        return this;
    }

    public LocalDate getOrderDate() {
        return orderDate;
    }

    public Sample setOrderDate(LocalDate orderDate) {
        this.orderDate = orderDate;
        return this;
    }

    public Integer getCt() {
        return ct;
    }

    public Sample setCt(Integer ct) {
        this.ct = ct;
        return this;
    }

    public String getViollierPlateName() {
        return viollierPlateName;
    }

    public Sample setViollierPlateName(String viollierPlateName) {
        this.viollierPlateName = viollierPlateName;
        return this;
    }

    public String getWellPosition() {
        return wellPosition;
    }

    public Sample setWellPosition(String wellPosition) {
        this.wellPosition = wellPosition;
        return this;
    }
}

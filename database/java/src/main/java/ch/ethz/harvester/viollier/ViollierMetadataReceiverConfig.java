package ch.ethz.harvester.viollier;

import ch.ethz.harvester.core.*;

import java.util.ArrayList;
import java.util.List;


public class ViollierMetadataReceiverConfig implements Config {

    public static class ViollierConfig {
        private String sampleMetadataDirPath;
        private List<String> gfbNotificationRecipients = new ArrayList<>();
        private List<String> fgczNotificationRecipients = new ArrayList<>();
        private List<String> h2030NotificationRecipients = new ArrayList<>();
        private List<String> additionalRecipients = new ArrayList<>();

        public String getSampleMetadataDirPath() {
            return sampleMetadataDirPath;
        }

        public ViollierConfig setSampleMetadataDirPath(String sampleMetadataDirPath) {
            this.sampleMetadataDirPath = sampleMetadataDirPath;
            return this;
        }

        public List<String> getGfbNotificationRecipients() {
            return gfbNotificationRecipients;
        }

        public void setGfbNotificationRecipients(List<String> gfbNotificationRecipients) {
            this.gfbNotificationRecipients = gfbNotificationRecipients;
        }

        public List<String> getFgczNotificationRecipients() {
            return fgczNotificationRecipients;
        }

        public void setFgczNotificationRecipients(List<String> fgczNotificationRecipients) {
            this.fgczNotificationRecipients = fgczNotificationRecipients;
        }

        public List<String> getH2030NotificationRecipients() {
            return h2030NotificationRecipients;
        }

        public ViollierConfig setH2030NotificationRecipients(List<String> h2030NotificationRecipients) {
            this.h2030NotificationRecipients = h2030NotificationRecipients;
            return this;
        }

        public List<String> getAdditionalRecipients() {
            return additionalRecipients;
        }

        public void setAdditionalRecipients(List<String> additionalRecipients) {
            this.additionalRecipients = additionalRecipients;
        }
    }

    private DatabaseConfig vineyard;
    private NotificationConfig notification;
    private ViollierConfig viollier;
    private LooperConfig looper;

    public DatabaseConfig getVineyard() {
        return vineyard;
    }

    public ViollierMetadataReceiverConfig setVineyard(DatabaseConfig vineyard) {
        this.vineyard = vineyard;
        return this;
    }

    public NotificationConfig getNotification() {
        return notification;
    }

    public ViollierMetadataReceiverConfig setNotification(NotificationConfig notification) {
        this.notification = notification;
        return this;
    }

    public ViollierConfig getViollier() {
        return viollier;
    }

    public ViollierMetadataReceiverConfig setViollier(ViollierConfig viollier) {
        this.viollier = viollier;
        return this;
    }

    public LooperConfig getLooper() {
        return looper;
    }

    public void setLooper(LooperConfig looper) {
        this.looper = looper;
    }
}

package de.intranda.goobi.plugins;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import net.xeoh.plugins.base.annotations.PluginImplementation;

import org.apache.log4j.Logger;
import org.goobi.production.cli.helper.WikiFieldHelper;
import org.goobi.production.enums.PluginGuiType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.enums.StepReturnValue;
import org.goobi.production.plugin.interfaces.IPlugin;
import org.goobi.production.plugin.interfaces.IStepPlugin;

import de.intranda.goobi.plugins.utils.ArchiveUtils;

import org.goobi.beans.Process;
import org.goobi.beans.Processproperty;
import org.goobi.beans.Step;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import dubious.sub.goobi.helper.encryption.MD5;

@PluginImplementation
public class ImageDeliveryPlugin implements IStepPlugin, IPlugin {

    private static final Logger log = Logger.getLogger(ImageDeliveryPlugin.class);
    
    private String pluginname = "plugin_intranda_imageDelivery";

    private Process process;
    private String returnPath;

    private static final String PROPERTYTITLE = "DOWNLOADURL";

    @Override
    public PluginType getType() {
        return PluginType.Step;
    }

    @Override
    public String getTitle() {
        return pluginname;
    }

    @Override
    public String getDescription() {
        return pluginname;
    }

    @Override
    public void initialize(Step step, String returnPath) {
        this.process = step.getProzess();
        this.returnPath = returnPath;
    }

    @Override
    public boolean execute() {

        MD5 md5 = new MD5(process.getTitel());
        String imagesFolderName = "";

        try {
            imagesFolderName = process.getImagesTifDirectory(false);
        } catch (SwapException | DAOException | IOException | InterruptedException e1) {
            log.error(e1);
        }

        File compressedFile =
                new File(ConfigPlugins.getPluginConfig(this).getString("destinationFolder", "/opt/digiverso/goobi/export/"), md5.getMD5() + "_"
                        + process.getTitel() + ".zip");

        File imageFolder = new File(imagesFolderName);
        File[] filenames = imageFolder.listFiles(Helper.dataFilter);
        if ((filenames == null) || (filenames.length == 0)) {
            return false;
        }
//        File destFile =
//                new File(ConfigPlugins.getPluginConfig(this).getString("destinationFolder", "/opt/digiverso/goobi/export/"), compressedFile.getName());

        log.debug("Found " + filenames.length + " files.");

        try {
            ArchiveUtils.zipFiles(filenames, compressedFile);
        } catch (IOException e) {
            log.error("Failed to zip files to archive for " + process.getTitel() + ". Aborting.");
            return false;
        }

//        log.info("Validating zip-archive");
//        byte[] origArchiveAfterZipChecksum = null;
//        try {
//            origArchiveAfterZipChecksum = ArchiveUtils.createChecksum(compressedFile);
//        } catch (NoSuchAlgorithmException | IOException e) {
//            log.error(process.getTitel() + ": " + "Failed to validate zip archive: " + e.toString() + ". Aborting.");
//            return false;
//        }

        if (ArchiveUtils.validateZip(compressedFile, true, imageFolder, filenames.length)) {
            log.info("Zip archive for " + process.getTitel() + " is valid");
        } else {
            log.error(process.getTitel() + ": " + "Zip archive for " + process.getTitel() + " is corrupted. Aborting.");
            return false;
        }
        // ////////Done validating archive

        // ////////copying archive file and validating copy
        log.info("Copying zip archive for " + process.getTitel() + " to archive");
//        try {
//            ArchiveUtils.copyFile(compressedFile, destFile);
//            // validation
//            if (!MessageDigest.isEqual(origArchiveAfterZipChecksum, ArchiveUtils.createChecksum(destFile))) {
//                log.error(process.getTitel() + ": " + "Error copying archive file to archive: Copy is not valid. Aborting.");
//                return false;
//            }
//        } catch (IOException | NoSuchAlgorithmException e) {
//            log.error(process.getTitel() + ": " + "Error validating copied archive. Aborting.");
//            return false;
//        }
//        log.info("Zip archive copied to " + destFile.getAbsolutePath() + " and found to be valid.");

        //
        //        // - an anderen Ort kopieren
        //        String destination = ConfigPlugins.getPluginConfig(this).getString("destinationFolder", "/opt/digiverso/pdfexport/");
        String donwloadServer = ConfigPlugins.getPluginConfig(this).getString("donwloadServer", "http://amsterdam01.intranda.com/");
        String downloadUrl = donwloadServer + compressedFile.getName();

        // - Name/Link als Property speichern

        boolean matched = false;
        for (Processproperty pe : process.getEigenschaftenList()) {
            if (pe.getTitel().equals(PROPERTYTITLE)) {
                pe.setWert(downloadUrl);
                matched = true;
                break;
            }
        }

        if (!matched) {
            Processproperty pe = new Processproperty();
            pe.setTitel(PROPERTYTITLE);
            pe.setWert(downloadUrl);
            process.getEigenschaften().add(pe);
            pe.setProzess(process);
        }

        try {
            ProcessManager.saveProcess(process);
        } catch (DAOException e) {
            createMessages(process.getTitel() + ": " + Helper.getTranslation("fehlerNichtSpeicherbar"), e);
            return false;
        }

        return true;
    }

    @Override
    public String cancel() {
        return returnPath;

    }

    @Override
    public String finish() {
        return returnPath;

    }

    @Override
    public HashMap<String, StepReturnValue> validate() {
        return null;
    }

    @Override
    public Step getStep() {
        return null;
    }

    @Override
    public PluginGuiType getPluginGuiType() {
        return PluginGuiType.NONE;
    }

    private void createMessages(String message, Exception e) {
        if (e != null) {
            Helper.setFehlerMeldung(message, e);
            ProcessManager.addLogfile(WikiFieldHelper.getWikiMessage(process.getWikifield(), "error", message), process.getId());
            log.error(message, e);
        } else {
            Helper.setFehlerMeldung(message);
            ProcessManager.addLogfile(WikiFieldHelper.getWikiMessage(process.getWikifield(), "error", message), process.getId());
            log.error(message);
        }

    }

    @Override
    public String getPagePath() {
        return null;
    }

}

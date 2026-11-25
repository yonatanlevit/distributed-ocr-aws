package dsp.ocr.common;

/** Placeholder shared class; proves the role modules can use code from common. */
public final class AppInfo {
    private AppInfo() {}

    public static String banner(String role) {
        return "[" + role + "] OCR-in-the-Cloud, running on Java " + Runtime.version();
    }
}

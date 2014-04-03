
// CS 511 Spring '14 with Prof. Kevin Chang
package teamdatum;

public interface FileService {
    public static final FileService singleton = FileServiceImpl.singleton;
    FileHandle open(String path, FileObserver observer);
    void shutdown();
}


// CS 511 Spring '14 with Prof. Kevin Chang
package teamdatum;

public interface FileObserver {
    // TODO: add onError(FileHandle handle, Exception error)
    public void onClose(FileHandle handle);
    public void onNext(FileHandle handle, String line);
    public void onOpen(FileHandle handle);
}

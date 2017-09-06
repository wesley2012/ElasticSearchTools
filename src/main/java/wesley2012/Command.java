package wesley2012;

/**
 * Created by gaolb on 9/6/17.
 */
public abstract class Command {
    abstract void help();
    abstract void parse(String[] args) throws Exception;
    abstract void exec();
}

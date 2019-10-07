import java.time.LocalDate;
import java.time.Period;
import java.util.Date;

public class User {
    private int uid;
    private String fn;
    private String state;
    private LocalDate dob;
    private String addr;
    private String city;

    public User(int uid, String fn, String state) {
        this.uid = uid;
        this.fn = fn;
        this.state = state;
    }

    public User(int uid, String addr, String city, String state, String dob){
        this.uid = uid;
        this.addr = addr;
        this.city = city;
        this.state = state;
        String[] temp = dob.split("/");
        int month = Integer.parseInt(temp[0]);
        int date = Integer.parseInt(temp[1]);
        int year = Integer.parseInt(temp[2]);
        this.dob = LocalDate.of(year, month, date);
    }

    public int getAge(){
        return Period.between(this.dob, LocalDate.now()).getYears();
    }

    public int getID(){
        return uid;
    }

    public String printAddr(){
        return this.addr + ", " + this.city + ", " + this.state;
    }

    @Override
    public String toString() {
        return this.fn + ":" + this.state;
    }

}

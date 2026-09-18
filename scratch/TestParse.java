import in.simplifymoney.ledgersync.parse.Amounts;
public class TestParse {
    public static void main(String[] args) {
        String body = "ICICI Bank Acct XX9075 Dr INR 2499.50 on 29-Jul-2026 09:13; UBER INDIA ref no 731877652068. BalAvl Rs 48,170.01";
        System.out.println("Stated Balance: " + Amounts.statedBalance(body));
    }
}

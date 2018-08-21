package lk.inli.xa;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class XaDsTxApplication {

	public static void main(String[] args) {
		SpringApplication.run(XaDsTxApplication.class, args);
	}

	public static final String TOPIC = "messages";

	@Service
	public static class MessageNotificationListner {

		@KafkaListener(topics = TOPIC, groupId = "test-id")
		public void onNewMessage(String id) {
			System.out.println("Message ID: " + id);
		}

	}

	@RestController
	public static class XaApiRestController {
		// @Autowired
		private KafkaTemplate<String, String> kafkaTemplate;
		private final JdbcTemplate jdbcTemplate;

		public XaApiRestController(KafkaTemplate<String, String> kafkaTemplate, JdbcTemplate jdbcTemplate) {
			this.kafkaTemplate = kafkaTemplate;
			this.jdbcTemplate = jdbcTemplate;
		}

		@GetMapping(path = "messages")
		public Collection<Map<String, String>> read() {
			return this.jdbcTemplate.query("select * from MESSAGE", (ResultSet resultSet, int i) -> {
				Map<String, String> msg = new HashMap<>();
				msg.put("id", resultSet.getString("ID"));
				msg.put("message", resultSet.getString("MESSAGE"));
				return msg;
			});
		}

		@PostMapping
		@Transactional
		public void write(@RequestBody Map<String, String> payload, @RequestParam Optional<Boolean> rollback) {
			String id = UUID.randomUUID().toString();
			String name = payload.get("name");
			String msg = "Hello, " + name + "!";

			this.jdbcTemplate.update("insert into MESSAGE(ID, MESSAGE) values(?, ?)", id, msg);

			this.kafkaTemplate.send(TOPIC, id);

			if (rollback.orElse(false)) {
				throw new RuntimeException("Couldn't write the message!");
			}
		}
	}
}

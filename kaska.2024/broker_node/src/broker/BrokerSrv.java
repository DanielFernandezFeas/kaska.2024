// Servidor que implementa la interfaz remota Kaska
package broker;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.List;
import kaskaprot.KaskaSrv;
import kaskaprot.TopicWithOffset;

class BrokerSrv extends UnicastRemoteObject implements KaskaSrv  {
    public static final long serialVersionUID=1234567890L;

    private final Map<String, List<byte[]>> topics;
    private final Map<String, Map<String, Integer>> subscripciones;

    public BrokerSrv() throws RemoteException {
        super();
        topics = new HashMap<>();
        subscripciones = new HashMap<>();
    }
    // se crean temas devolviendo cuántos se han creado
    public synchronized int createTopics(Collection <String> topics) throws RemoteException {
        int cuenta = 0;
        for (String topic : topics) {
            if (!this.topics.containsKey(topic)) {
                this.topics.put(topic, new LinkedList<>());
                cuenta++;
            }
        }
        return cuenta;
    }
    // devuelve qué temas existen
    public synchronized Collection<String> topicList() throws RemoteException {
        return new LinkedList<>(topics.keySet());
    }
    // envía un array bytes devolviendo error si el tema no existe
    public synchronized boolean send(String topic, byte[] message) throws RemoteException {
        List<byte[]> meesList = topics.get(topic);
        if (meesList == null) {
            return false; // El tema no existe
        }
        meesList.add(message);
        return true;
    }
    // lee un determinado mensaje de un tema devolviendo null si error
    // (tema no existe o mensaje no existe)
    // se trata de una función para probar el buen funcionamiento de send;
    // en Kafka los mensajes se leen con poll
    public synchronized byte[] get(String topic, int offset) throws RemoteException {
        List<byte[]> messList = topics.get(topic);
        if (messList == null || offset < 0 || offset >= messList.size()) {
            return null; // El tema no existe o el offset es inválido
        }
        return messList.get(offset);
    }
    // obtiene el offset actual de estos temas en el broker ignorando
    // los temas que no existen
    public synchronized Collection <TopicWithOffset> endOffsets(Collection <String> topics) throws RemoteException {
        return topics.stream().filter(this.topics::containsKey).map(topic -> new TopicWithOffset(topic, this.topics.get(topic).size())).collect(Collectors.toList());
    }
    // obtiene todos los mensajes no leídos de los temas suscritos
    public synchronized Map<String, List<byte[]>> poll(Collection<TopicWithOffset> topicOffsets) throws RemoteException {
        Map<String, List<byte[]>> mess = new HashMap<>();
        for (TopicWithOffset subscription : topicOffsets) {
            String tp = subscription.getTopic();
            int offset = subscription.getOffset();
            List<byte[]> messList = topics.get(tp);
            if (messList != null && offset < messList.size()) {
                mess.put(tp, new LinkedList<>(messList.subList(offset, messList.size())));
            }
        }
        return mess;
    }
    static public void main (String args[])  {
        if (args.length!=1) {
            System.err.println("Usage: BrokerSrv registryPortNumber");
            return;
        }
        try {
            BrokerSrv srv = new BrokerSrv();
            Registry registry = LocateRegistry.getRegistry(Integer.parseInt(args[0]));
            registry.rebind("KaskaSrv", srv);
        }
        catch (Exception e) {
            System.err.println("Broker exception: " + e.toString());
            System.exit(1);
        }
    }
}

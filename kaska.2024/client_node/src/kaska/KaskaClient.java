package kaska;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import kaskaprot.KaskaSrv;
import kaskaprot.TopicWithOffset;

public class KaskaClient {
    // Atributos
    private KaskaSrv server;
    private Map<String, Integer> subscripciones; // Mapa local para almacenar suscripciones y offsets

    // Constructor

    public KaskaClient(String host, String port) throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(host, Integer.parseInt(port));
        server = (KaskaSrv) registry.lookup("KaskaSrv");
    }

    // Métodos
    // Crea temas devolviendo cuántos se han creado
    public int createTopics(Collection<String> topics) throws RemoteException {
        HashSet<String> topicSet = new HashSet<>(topics);
        return server.createTopics(topicSet);
    }

    // Función de conveniencia para crear un solo tema
    public boolean createOneTopic(String topic) throws RemoteException {
        return createTopics(Arrays.asList(topic)) == 1;
    }

    // Devuelve qué temas existen
    public Collection<String> topicList() throws RemoteException {
        return server.topicList();
    }

    // Envía un mensaje devolviendo error si el tema no existe o el objeto no es serializable
    public boolean send(String topic, Object o) throws RemoteException {
        byte[] serializedMessage = serializeMessage(o);
        if (serializedMessage == null) {
            return false; // El objeto no es serializable
        }
        return server.send(topic, serializedMessage);
    }

    // Lee mensaje pedido de un tema devolviéndolo en un Record (null si error: tema no existe o mensaje no existe)
    public Record get(String topic, int offset) throws RemoteException {
        byte[] mensaje = server.get(topic, offset);
        if (mensaje == null) {
        return null; // Error: tema no existe o mensaje no existe
    }
    return new Record(topic, offset, mensaje);
    }

    // Se suscribe a los temas pedidos devolviendo a cuántos se ha suscrito
    public int subscribe(Collection<String> topics) throws RemoteException {
        int cuenta = 0;
        for (String topic : topics) {
            if (!subscripciones.containsKey(topic)) {
                subscripciones.put(topic, 0); // Asume un offset inicial de 0
                cuenta++;
            }
        }
        return cuenta;
    }

    // Función de conveniencia para suscribirse a un solo tema
    public boolean subscribeOneTopic(String topic) throws RemoteException {
        return subscribe(Arrays.asList(topic)) == 1;
    }

    // Se da de baja de todas las suscripciones
    public void unsubscribe() {
        // Local method to clear local state
        // (Implementation depends on how you manage subscripciones locally)
        subscripciones.clear();
    }

    // Obtiene el offset local de un tema devolviendo -1 si no está suscrito a ese tema
    public int position(String topic) {
        // Local method to get the position
        // (Implementation depends on how you manage offsets locally)
        return subscripciones.getOrDefault(topic, -1);
        }

    // Modifica el offset local de un tema devolviendo error si no está suscrito a ese tema
    public boolean seek(String topic, int offset) {
        // Local method to set the offset
        // (Implementation depends on how you manage offsets locally)
        if (subscripciones.containsKey(topic)) {
            subscripciones.put(topic, offset);
            return true;
        }
        return false;
        }

    // Obtiene todos los mensajes no leídos de los temas suscritos, devuelve null si no está suscrito a ningún tema
    public List<Record> poll() throws RemoteException {
        Collection<TopicWithOffset> topicOffsets = new HashSet<>();
        for (Map.Entry<String, Integer> entry : subscripciones.entrySet()) {
            topicOffsets.add(new TopicWithOffset(entry.getKey(), entry.getValue()));
        }
        Map<String, List<byte[]>> MessMap = server.poll(topicOffsets);
        List<Record> re = new ArrayList<>();
        for (Map.Entry<String, List<byte[]>> entry : MessMap.entrySet()) {
            String tp = entry.getKey();
            List<byte[]> messages = entry.getValue();
            int offset = subscripciones.get(tp);
            for (int i = 0; i < messages.size(); i++) {
                re.add(new Record(tp, offset + i, messages.get(i)));
            }
            subscripciones.put(tp, offset + messages.size()); // Actualiza el offset local
        }
        return re;
    }

    // Función interna que serializa un objeto en un array de bytes, devuelve null si el objeto no es serializable
    private byte[] serializeMessage(Object o) {
        byte[] byt = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            byt = bos.toByteArray();
        } catch (IOException e) {
            System.err.println("error en la serialización " + e.toString());
        }
        return byt;
    }
}

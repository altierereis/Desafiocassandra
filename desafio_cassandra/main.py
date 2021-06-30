from flask import Flask, request, jsonify
from cassandra.cluster import Cluster #pip install cassandra-driver
cluster = Cluster(['ip da aws'], port=9042)
session = cluster.connect('estoque', wait_for_all_pools=True)
session.execute('use estoque')

app = Flask(__name__)


@app.route("/api/produtos", methods=["GET"]) #GET: Usado para leitura de registros
def GetProdutos():
    lista_prod = []
    rows = session.execute('select id, fabricante, nome, cast(valor as float), estoque, estoqueminimo, estoquemaximo, variacoes from produtos')
    for row in rows:
        lista_prod.append(row)
    return jsonify(lista_prod)

@app.route("/api/produtos/<id>", methods=["GET"])
def GetProduto(id):

    query = session.prepare('select id, fabricante, nome, cast(valor as float), estoque, estoqueminimo, estoquemaximo, variacoes from produtos where id= ?')
    rows = session.execute(query, [int(id)])

    for row in rows:
        return jsonify([row])

    return jsonify([])


@app.route("/api/produtos", methods=["POST"]) #Cadastrar registros na API
def CadastrarProduto():
    bodyData = request.get_json()
    query = session.prepare("insert into produtos (id, fabricante, nome, valor, estoque, estoqueminimo, estoquemaximo, "
                            "variacoes) values (?, ?, ?, ?, ?, ?, ?, ?)")
    produto = session.execute(query, [bodyData["id"], bodyData["fabricante"], bodyData["nome"], bodyData["valor"], bodyData["estoque"],
                                      bodyData["estoqueminimo"], bodyData["estoquemaximo"], bodyData["variacoes"]])

    return jsonify({"message": "Produto incluído com sucesso", "Produto": bodyData})


@app.route("/api/produtos/<id>", methods=["PUT"]) #Atualizar registros na API
def AtualizarProduto(id):
     bodyData = request.get_json()
     query = session.prepare("update produtos set estoque = ? where id = ? and fabricante = ?")
     produto = session.execute(query, [bodyData["estoque"], int(id), bodyData["fabricante"]])

     return jsonify({"message": f"Produto {id} atualizado com sucesso", "produto_atualizado": bodyData})

@app.route("/api/produtos/<id>/<fabricante>", methods=["DELETE"]) #Remover registro na API
def RemoverProduto(id, fabricante):
    query = session.prepare("delete from produtos where id = ? and fabricante = ?")
    produto = session.execute(query, [int(id), fabricante])

    return jsonify({"message": f"Produto {id} removido com sucesso"})



if __name__ == "__main__":
    app.run()

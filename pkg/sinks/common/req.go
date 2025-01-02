package common

type Request struct {
	Namespace string
	Id        interface{}
	Doc       interface{}
	Date      string
}

func (r Request) GetNamespace() string {
	return r.Namespace
}

func (r Request) GetId() interface{} {
	return r.Id
}

func (r Request) GetDoc() interface{} {
	return r.Doc
}

func (r Request) GetDate() string {
	return r.Date
}

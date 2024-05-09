package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/brevdev/dev-plane/pkg/collections"
	"github.com/brevdev/dev-plane/pkg/errors"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/iancoleman/strcase"
	flag "github.com/spf13/pflag"
)

func main() { //nolint:funlen //main
	name := ""
	flag.StringVar(&name, "name", "", "name you want to generate with")
	path := ""
	flag.StringVar(&path, "path", "internal", "path you want to generate with")

	full := false
	flag.BoolVar(&full, "full", false, "generate all parts of a service")
	service := ""
	flag.StringVar(&service, "service", "", "generate the service")
	flag.Lookup("service").NoOptDefVal = "-"
	entity := ""
	flag.StringVar(&entity, "entity", "", "generate the entity")
	flag.Lookup("entity").NoOptDefVal = "-"
	globalID := ""
	flag.StringVar(&globalID, "globalId", "", "generate a global id")
	flag.Lookup("globalId").NoOptDefVal = "-"
	repo := ""
	flag.StringVar(&repo, "repo", "", "generate the repo")
	flag.Lookup("repo").NoOptDefVal = "-"
	api := ""
	flag.StringVar(&api, "api", "", "generate the api")
	flag.Lookup("api").NoOptDefVal = "-"
	workflow := ""
	flag.StringVar(&workflow, "workflow", "", "generate the api")
	flag.Lookup("workflow").NoOptDefVal = "-"

	proto := ""
	flag.StringVar(&proto, "proto", "", "path you want to generate with")
	flag.Lookup("proto").NoOptDefVal = "-"
	ent := ""
	flag.StringVar(&ent, "ent", "", "path you want to generate with")
	flag.Lookup("ent").NoOptDefVal = "-"

	flag.Parse()

	if full {
		err := getAllBrevService(GenBrevServiceArgs{
			Path:         path,
			PackageName:  name,
			ServiceName:  optionalFlagToValue("service", service),
			Entity:       optionalFlagToValue("entity", entity),
			RepoName:     optionalFlagToValue("repo", repo),
			APIName:      optionalFlagToValue("api", api),
			WorkflowName: optionalFlagToValue("workflow", workflow),
		})
		if err != nil {
			panic(err)
		}
		err = GenID(GenIDArgs{
			Name: name,
			Path: path,
		})
		if err != nil {
			panic(err)
		}
		err = GenEnt(GenEntArgs{
			Name: name,
			Path: "schema",
		})
		if err != nil {
			panic(err)
		}
		err = GenProto(GenProtoArgs{
			Name:        name,
			Path:        "devplaneapis",
			PackageName: "devplaneapi",
			Version:     "v1",
		})
		if err != nil {
			panic(err)
		}
		return
	}

	if optionalFlagToValue("proto", proto) != nil {
		err := GenProto(GenProtoArgs{
			Name:        name,
			Path:        path,
			PackageName: "devplaneapi",
			Version:     "v1",
		})
		if err != nil {
			panic(err)
		}
	}
	if optionalFlagToValue("ent", ent) != nil {
		err := GenEnt(GenEntArgs{})
		if err != nil {
			panic(err)
		}
	}

	if isOneofFlagPassed([]string{"service", "repo", "api", "workflow"}) {
		err := genBrevService(GenBrevServiceArgs{
			Path:         path,
			PackageName:  name,
			ServiceName:  optionalFlagToValue("service", service),
			Entity:       optionalFlagToValue("entity", entity),
			RepoName:     optionalFlagToValue("repo", repo),
			APIName:      optionalFlagToValue("api", api),
			WorkflowName: optionalFlagToValue("workflow", workflow),
		})
		if err != nil {
			panic(err)
		}
		return
	}

	err := genPackage(GenPackageArgs{
		Name: name,
		Path: path,
	})
	if err != nil {
		panic(err)
	}
}

func optionalFlagToValue(name, flagValue string) *string {
	isFlagProvided := isFlagPassed(name)
	if flagValue == "-" || flagValue == "" {
		if isFlagProvided {
			emptyStr := ""
			return &emptyStr
		} else {
			return nil
		}
	}
	return &flagValue
}

func isOneofFlagPassed(names []string) bool {
	for _, name := range names {
		if isFlagPassed(name) {
			return true
		}
	}
	return false
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

type GenBrevServiceArgs struct {
	Path         string
	PackageName  string
	ServiceName  *string
	Entity       *string
	RepoName     *string
	APIName      *string
	WorkflowName *string
}

func (g GenBrevServiceArgs) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.Path, validation.Required),
		validation.Field(&g.PackageName, validation.Required),
	)
}

func getAllBrevService(args GenBrevServiceArgs) error {
	args.APIName = collections.Ptr(collections.DefaultPtrOrValue(args.APIName, args.PackageName))
	args.ServiceName = collections.Ptr(collections.DefaultPtrOrValue(args.ServiceName, args.PackageName))
	args.Entity = collections.Ptr(collections.DefaultPtrOrValue(args.Entity, args.PackageName))
	args.RepoName = collections.Ptr(collections.DefaultPtrOrValue(args.RepoName, args.PackageName))
	args.WorkflowName = collections.Ptr(collections.DefaultPtrOrValue(args.WorkflowName, args.PackageName))

	err := genBrevService(args)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func genBrevService(args GenBrevServiceArgs) error { //nolint:funlen //main
	err := args.Validate()
	if err != nil {
		return errors.WrapAndTrace(err)
	}

	goPackageName := toGoPackageName(args.PackageName)
	packageDir := args.Path + "/" + goPackageName

	err = os.MkdirAll(packageDir, os.ModePerm)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	if args.APIName != nil {
		name := collections.DefaultValue(*args.APIName, args.PackageName)
		fileContent := makeAPIFile(goPackageName, name)
		testContent := makeAPITestFile(goPackageName, name)
		err = genGoFileWithTestContent("api", packageDir, fileContent, testContent)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
	}
	if args.ServiceName != nil {
		name := collections.DefaultValue(*args.ServiceName, args.PackageName)
		fileContent := makeServiceFile(goPackageName, name)
		testContent := fmt.Sprintf(packageFileHeaderTemplate, goPackageName)
		err = genGoFileWithTestContent("service", packageDir, fileContent, testContent)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
	}
	if args.Entity != nil {
		name := collections.DefaultValue(*args.Entity, args.PackageName)
		fileContent := makeEntityFile(goPackageName, name)
		testContent := fmt.Sprintf(packageFileHeaderTemplate, goPackageName)
		err = genGoFileWithTestContent("entity", packageDir, fileContent, testContent)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
	}
	if args.RepoName != nil {
		name := collections.DefaultValue(*args.RepoName, args.PackageName)
		fileContent := makeRepoFile(goPackageName, name)
		testContent := makeRepoTestFile(goPackageName, name)
		err = genGoFileWithTestContent("repository", packageDir, fileContent, testContent)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
	}
	if args.WorkflowName != nil {
		name := collections.DefaultValue(*args.WorkflowName, args.PackageName)
		fileContent := makeWorkflowFile(goPackageName, name)
		testContent := makeWorkflowTestFile(goPackageName, name)
		err = genGoFileWithTestContent("workflow", packageDir, fileContent, testContent)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
	}
	return nil
}

type GenIDArgs struct {
	Name string
	Path string
}

func (g GenIDArgs) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.Name, validation.Required),
		validation.Field(&g.Path, validation.Required),
	)
}

func GenID(args GenIDArgs) error {
	name := strcase.ToCamel(args.Name)
	idVar := fmt.Sprintf(idTemplate, name)
	packagePath := args.Path + "/" + "ids"
	filePath := packagePath + "/ids.go"
	res, err := os.ReadFile(filePath) //nolint:gosec //script
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	if strings.Contains(string(res), idVar) {
		return nil
	}

	idSnippet := makeIDSnippet(name)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0o644) //nolint:gosec //script
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	_, err = f.WriteString(idSnippet)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = f.Close()
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

type GenProtoArgs struct {
	Name        string
	Path        string
	PackageName string
	Version     string
}

func (g GenProtoArgs) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.Name, validation.Required),
		validation.Field(&g.Path, validation.Required),
	)
}

func GenProto(args GenProtoArgs) error {
	err := args.Validate()
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	packageName := args.PackageName
	version := args.Version
	protoFileContent := makeProtoFile(packageName, version, args.Name)

	protoPath := args.Path + "/" + packageName + "/" + version + "/" + strcase.ToSnake(args.Name) + ".proto"
	err = writeFile(protoPath, protoFileContent)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = exec.Command("make", "generate-proto").Run()
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

type GenEntArgs struct {
	Name string
	Path string
}

func (g GenEntArgs) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.Name, validation.Required),
		validation.Field(&g.Path, validation.Required),
	)
}

const goSuffix = ".go"

func GenEnt(args GenEntArgs) error {
	err := args.Validate()
	if err != nil {
		return errors.WrapAndTrace(err)
	}

	entFileContent := makeEntFile(args.Name)
	entPath := args.Path + "/" + toGoPackageName(args.Name) + goSuffix
	err = writeFile(entPath, entFileContent)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = exec.Command("make", "generate-ent").Run()
	if err != nil {
		return errors.WrapAndTrace(err)
	}

	return nil
}

type GenPackageArgs struct {
	Name string
	Path string
}

func (g GenPackageArgs) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.Name, validation.Required),
		validation.Field(&g.Path, validation.Required),
	)
}

func genPackage(args GenPackageArgs) error {
	err := args.Validate()
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	packageName := toGoPackageName(args.Name)

	packageDir := args.Path + "/" + packageName
	err = os.MkdirAll(packageDir, os.ModePerm)
	if err != nil {
		return errors.WrapAndTrace(err)
	}

	fileContent := fmt.Sprintf(packageFileHeaderTemplate, packageName)
	err = genGoFileWithTest(packageName, packageDir, fileContent)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func genGoFileWithTestContent(name string, packageDir string, mainFileContent string, testFileContent string) error {
	packageFile := packageDir + "/" + name + goSuffix
	testFile := packageDir + "/" + name + "_test.go"
	err := writeFile(packageFile, mainFileContent)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = writeFile(testFile, testFileContent)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func genGoFileWithTest(name string, packageDir string, fileContent string) error {
	packageFile := packageDir + "/" + name + goSuffix
	testFile := packageDir + "/" + name + "_test.go"
	err := writeFile(packageFile, fileContent)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = writeFile(testFile, fileContent)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func writeFile(path, content string) error {
	f, err := os.Create(path) //nolint:gosec // this is a script
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	defer errors.HandleErrDefer(f.Close)
	_, err = f.Write([]byte(content))
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func toGoPackageName(name string) string {
	return strings.ToLower(strcase.ToCamel(name))
}

func ToGoPublicSymbolName(name string) string {
	return name
}

func ToGoPrivateSymbolName(name string) string {
	return name
}

func toPrefix(name string) string {
	return strings.ToLower(name)
}

func makeProtoFile(packageName string, version string, serviceName string) string {
	upperCamel := strcase.ToCamel(serviceName)
	snakeCase := strcase.ToSnake(serviceName)
	return fmt.Sprintf(protoFileTemplate, packageName, version, upperCamel, snakeCase)
}

func makeEntityFile(packageName string, entityName string) string {
	lowerCamel := strcase.ToLowerCamel(entityName)
	upperCamel := strcase.ToCamel(entityName)
	return fmt.Sprintf(packageFileHeaderTemplate+entityFileTemplate, packageName, upperCamel, lowerCamel, toPrefix(entityName))
}

func makeRepoFile(packageName string, repoName string) string {
	lowerCamel := strcase.ToLowerCamel(repoName)
	upperCamel := strcase.ToCamel(repoName)
	return fmt.Sprintf(packageFileHeaderTemplate+repositoryFileTemplate, packageName, upperCamel, lowerCamel, toPrefix(repoName))
}

func makeRepoTestFile(packageName string, repoName string) string {
	lowerCamel := strcase.ToLowerCamel(repoName)
	upperCamel := strcase.ToCamel(repoName)
	return fmt.Sprintf(packageFileHeaderTemplate+repoTestTemplate, packageName, upperCamel, lowerCamel, toPrefix(repoName))
}

func makeWorkflowFile(packageName string, repoName string) string {
	lowerCamel := strcase.ToLowerCamel(repoName)
	upperCamel := strcase.ToCamel(repoName)
	return fmt.Sprintf(packageFileHeaderTemplate+workflowFileTemplate, packageName, upperCamel, lowerCamel, toPrefix(repoName))
}

func makeWorkflowTestFile(packageName string, repoName string) string {
	lowerCamel := strcase.ToLowerCamel(repoName)
	upperCamel := strcase.ToCamel(repoName)
	return fmt.Sprintf(packageFileHeaderTemplate+workflowTestFileTemplate, packageName, upperCamel, lowerCamel, toPrefix(repoName))
}

func makeServiceFile(packageName string, serviceName string) string {
	lowerCamel := strcase.ToLowerCamel(serviceName)
	upperCamel := strcase.ToCamel(serviceName)
	return fmt.Sprintf(packageFileHeaderTemplate+serviceFileTemplate, packageName, upperCamel, lowerCamel, toPrefix(serviceName))
}

func makeAPIFile(packageName string, apiName string) string {
	lowerCamel := strcase.ToLowerCamel(apiName)
	upperCamel := strcase.ToCamel(apiName)
	return fmt.Sprintf(packageFileHeaderTemplate+apiFileTemplate, packageName, upperCamel, lowerCamel, toPrefix(apiName))
}

func makeAPITestFile(packageName string, apiName string) string {
	lowerCamel := strcase.ToLowerCamel(apiName)
	upperCamel := strcase.ToCamel(apiName)
	return fmt.Sprintf(packageFileHeaderTemplate+apiTestTemplate, packageName, upperCamel, lowerCamel, toPrefix(apiName))
}

func makeIDSnippet(name string) string {
	upperCamel := strcase.ToCamel(name)
	return fmt.Sprintf(idFileSnippet, fmt.Sprintf(idTemplate, upperCamel))
}

func makeEntFile(name string) string {
	upperCamel := strcase.ToCamel(name)
	return fmt.Sprintf(entFileTemplate, upperCamel)
}

// 1 = package name
// 2 = public name
// 3 = private name
// 4 = prefix

var packageFileHeaderTemplate = `package %[1]s
`

var (
	idTemplate    = `%sID`
	idFileSnippet = `
type %s prefixid.PrefixID
`
)

var entityFileTemplate = `
import (
	"github.com/brevdev/dev-plane/internal/ids"
	"github.com/brevdev/dev-plane/internal/labels"
	"github.com/brevdev/dev-plane/pkg/commonentity"
	"github.com/brevdev/dev-plane/pkg/pagination"
	"github.com/brevdev/dev-plane/pkg/prefixid"
)

type (
	%[2]s struct {
		commonentity.Time
		ID     ids.%[2]sID
		Labels labels.Labels
	}
	%[2]sPage pagination.Page[%[2]s]
)

func New%[2]s() %[2]s {
	return %[2]s{
		Time: commonentity.NewTime(),
		ID:   New%[2]sID(),
	}
}

const %[2]sPrefix = "%[4]s"

func New%[2]sID() ids.%[2]sID {
	return ids.%[2]sID(prefixid.New(%[2]sPrefix))
}

func (c *%[2]s) SetID(id string) {
	c.ID = ids.%[2]sID(id)
}

func (c *%[2]s) WithLabels(labels labels.Labels) *%[2]s {
	c.Labels = labels
	return c
}
`

var repositoryFileTemplate = `
import (
	"context"

	"entgo.io/ent/dialect/sql"
	"github.com/brevdev/dev-plane/ent"
	ent%[1]s "github.com/brevdev/dev-plane/ent/%[1]s"
	"github.com/brevdev/dev-plane/ent/predicate"
	"github.com/brevdev/dev-plane/internal/ids"
	"github.com/brevdev/dev-plane/internal/labels"
	"github.com/brevdev/dev-plane/internal/transaction"
	"github.com/brevdev/dev-plane/pkg/collections"
	"github.com/brevdev/dev-plane/pkg/errors"
	"github.com/brevdev/dev-plane/pkg/ordering"
	"github.com/brevdev/dev-plane/pkg/pagination"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type EntRepository struct {
	transaction.TransactionClient
}

var _ Repository = EntRepository{}

func NewEntRepository(client *ent.Client) EntRepository {
	return EntRepository{
		TransactionClient: transaction.TransactionClient{
			Client: client,
		},
	}
}

// return client or transaction client
func (r EntRepository) Get%[2]sClient(ctx context.Context) (*ent.%[2]sClient, error) {
	tx, err := transaction.GetEntTxFromCtx(ctx)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	if tx != nil {
		return tx.%[2]s, nil
	} else {
		return r.Client.%[2]s, nil
	}
}

func (r EntRepository) Get(ctx context.Context, id ids.%[2]sID) (%[2]s, error) {
	client, err := r.Get%[2]sClient(ctx)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	u, err := client.
		Query().
		Where(ent%[1]s.ID(id)).
		Only(ctx)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	res, err := MapEnt%[2]sTo%[2]s(u)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	return res, nil
}

type ListRepo%[2]sOptions struct {
	HasAllLabels   labels.Labels
	HasOneOfLabels labels.MultiLabels
	OrderOptions   %[2]sOrderOptions
}

func (i *ListRepo%[2]sOptions) SetDefault() {
	i.OrderOptions.SetDefault()
}

func (i ListRepo%[2]sOptions) Validate() error {
	return i.OrderOptions.Validate()
}

type %[2]sOrderOptions struct {
	ordering.OrderOptions
}

func (i *%[2]sOrderOptions) SetDefault() {
	i.OrderBy = collections.DefaultValue(i.OrderBy, ent%[1]s.FieldCreateTime)
	i.OrderOptions.SetDefaults()
}

func (i %[2]sOrderOptions) Validate() error {
	validColumns := ent%[1]s.Columns
	return validation.ValidateStruct(&i,
		validation.Field(&i.OrderBy, validation.Required, validation.In(collections.ListOfSomethingToListOfAny(validColumns)...)),
		validation.Field(&i.OrderOptions),
	)
}

// TODO make generic (only needs pred) -- tried it and it became very verbose since you need to transform types
// It may be easier for now to just copy this func per entity and replace predicate.<type>
func MultiPredicate[T any](
	params []T,
	preds []predicate.%[2]s,
	trans func(T) predicate.%[2]s,
	comb func(predicates ...predicate.%[2]s) predicate.%[2]s,
) []predicate.%[2]s {
	pr := []predicate.%[2]s{}
	for _, p := range params {
		pr = append(pr, trans(p))
	}
	preds = applyPredIfGreaterThan0(pr, preds, comb)
	return preds
}

func applyPredIfGreaterThan0(toAdd []predicate.%[2]s, preds []predicate.%[2]s, comb func(predicates ...predicate.%[2]s) predicate.%[2]s) []predicate.%[2]s {
	if len(toAdd) > 0 {
		preds = append(preds, comb(toAdd...))
	}
	return preds
}

func genericPredicateTo%[2]sPredicate(ps []func(*sql.Selector)) []predicate.%[2]s {
	pNew := []predicate.%[2]s{}
	for _, p := range ps {
		pNew = append(pNew, p)
	}
	return pNew
}

func (r EntRepository) List(ctx context.Context, options ListRepo%[2]sOptions, pageParams pagination.PageParams) (%[2]sPage, error) {
	pageParams.SetDefault()
	err := pageParams.Validate()
	if err != nil {
		return %[2]sPage{}, errors.WrapAndTrace(err)
	}
	options.SetDefault()
	err = options.Validate()
	if err != nil {
		return %[2]sPage{}, errors.WrapAndTrace(err)
	}

	predicates := []predicate.%[2]s{}

	hasLabelsPredicates := genericPredicateTo%[2]sPredicate(options.HasAllLabels.ToPredicates(ent%[1]s.FieldLabels))
	predicates = applyPredIfGreaterThan0(hasLabelsPredicates, predicates, ent%[1]s.And)

	hasOneOfLabelsPredicates := genericPredicateTo%[2]sPredicate(options.HasOneOfLabels.ToPredicates(ent%[1]s.FieldLabels))
	predicates = applyPredIfGreaterThan0(hasOneOfLabelsPredicates, predicates, ent%[1]s.Or)

	client, err := r.Get%[2]sClient(ctx)
	if err != nil {
		return %[2]sPage{}, errors.WrapAndTrace(err)
	}
	query := client.
		Query().
		Where(ent%[1]s.And(
			predicates...,
		)).
		Modify(
			func(s *sql.Selector) {
				options.OrderOptions.GetOrdering()(s)
			},
		)

	res, err := pagination.ApplyPagination(
		func(o int) { query.Offset(o) },
		func(l int) { query.Limit(l) },
		func() ([]*ent.%[2]s, error) { return query.All(ctx) }, //nolint:wrapcheck // one-liner
		MapEnt%[2]sTo%[2]ss,
		pageParams,
	)
	if err != nil {
		return %[2]sPage{}, errors.WrapAndTrace(err)
	}

	return %[2]sPage(res), nil
}

func (r EntRepository) Create(ctx context.Context, %[3]s %[2]s) (%[2]s, error) {
	client, err := r.Get%[2]sClient(ctx)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	ui := client.
		Create().
		SetID(%[3]s.ID).
		SetLabels(%[3]s.Labels).
		SetCreateTime(%[3]s.CreateTime).
		SetUpdateTime(%[3]s.UpdateTime)
	u, err := ui.Save(ctx)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	res, err := MapEnt%[2]sTo%[2]s(u)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	return res, nil
}

func (r EntRepository) Delete(ctx context.Context, id ids.%[2]sID) error {
	client, err := r.Get%[2]sClient(ctx)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = client.
		DeleteOneID(id).
		Exec(ctx)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func MapEnt%[2]sTo%[2]s(ent%[2]s *ent.%[2]s) (%[2]s, error) {
	c, err := collections.TryCopyToNew[*ent.%[2]s, %[2]s](ent%[2]s)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	return c, nil
}

func MapEnt%[2]sTo%[2]ss(ent%[2]ss []*ent.%[2]s) ([]%[2]s, error) {
	%[3]ss, err := collections.MapE(ent%[2]ss, MapEnt%[2]sTo%[2]s)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return %[3]ss, nil
}

func Map%[2]sToEnt%[2]s(%[3]s %[2]s) (ent.%[2]s, error) {
	e, err := collections.TryCopyToNew[%[2]s, ent.%[2]s](%[3]s)
	if err != nil {
		return ent.%[2]s{}, errors.WrapAndTrace(err)
	}
	return e, nil
}
`

var serviceFileTemplate = `
import (
	"context"

	devplaneapiv1 "github.com/brevdev/dev-plane/gen/proto/go/devplaneapi/v1"
	"github.com/brevdev/dev-plane/internal/apihelpers"
	"github.com/brevdev/dev-plane/internal/ids"
	"github.com/brevdev/dev-plane/pkg/collections"
	"github.com/brevdev/dev-plane/pkg/errors"
	"github.com/brevdev/dev-plane/pkg/pagination"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type Repository interface {
	Get(ctx context.Context, id ids.%[2]sID) (%[2]s, error)
	List(ctx context.Context, options ListRepo%[2]sOptions, pageParams pagination.PageParams) (%[2]sPage, error)
	Create(ctx context.Context, %[3]s %[2]s) (%[2]s, error)
	Delete(ctx context.Context, id ids.%[2]sID) error
}

type WorkflowClient interface {
	ExecuteThing%[2]s(ctx context.Context) error
}

type DummyWorkflowClient struct{}

var _ WorkflowClient = DummyWorkflowClient{}

func (d DummyWorkflowClient) ExecuteThing%[2]s(_ context.Context) error {
	return nil
}

type Service struct {
	repository     Repository
	workflowClient WorkflowClient
}

func NewService(repo Repository, workflowClient WorkflowClient) Service {
	return Service{repo, workflowClient}
}

func (s Service) Get%[2]s(ctx context.Context, id ids.%[2]sID) (%[2]s, error) {
	err := validation.Required.Error("id is required").Validate(id)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	res, err := s.repository.Get(ctx, id)
	if err != nil {
		return res, errors.WrapAndTrace(err)
	}
	return res, nil
}

type List%[2]sOptions devplaneapiv1.List%[2]sOptions

func MapList%[2]sOptions(options *List%[2]sOptions) (ListRepo%[2]sOptions, error) {
	if options == nil {
		return ListRepo%[2]sOptions{}, nil
	}
	ilo, err := collections.TryCopyToNew[*List%[2]sOptions, ListRepo%[2]sOptions](options)
	if err != nil {
		return ilo, errors.WrapAndTrace(err)
	}
	ilo.HasOneOfLabels = apihelpers.MapAPIMultiLabelToMultiLabel(options.HasOneOfLabels)
	return ilo, nil
}

func (s Service) List%[2]s(ctx context.Context, options *List%[2]sOptions, pageParams pagination.PageParams) (%[2]sPage, error) {
	repoOptions, err := MapList%[2]sOptions(options)
	if err != nil {
		return %[2]sPage{}, errors.WrapAndTrace(err)
	}
	res, err := s.repository.List(ctx, repoOptions, pageParams)
	if err != nil {
		return %[2]sPage{}, errors.WrapAndTrace(err)
	}
	return res, nil
}

type Create%[2]sRequest struct {
	*devplaneapiv1.Create%[2]sRequest
}

func (c *Create%[2]sRequest) Validate() error {
	return validation.ValidateStruct(c)
}

func (s Service) Create%[2]s(ctx context.Context, create%[2]s *Create%[2]sRequest) (%[2]s, error) {
	err := create%[2]s.Validate()
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	%[3]s := New%[2]s()
	%[3]s.WithLabels(create%[2]s.Labels)
	if create%[2]s.%[2]sId != "" {
		%[3]s.SetID(create%[2]s.%[2]sId)
	}
	%[3]s, err = s.repository.Create(ctx, %[3]s)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	return %[3]s, nil
}

func (s Service) Delete%[2]s(ctx context.Context, id ids.%[2]sID) error {
	err := validation.Required.Error("id is required").Validate(id)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = s.repository.Delete(ctx, id)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}
`

var apiFileTemplate = `
import (
	"context"

	"connectrpc.com/connect"
	devplaneapiv1 "github.com/brevdev/dev-plane/gen/proto/go/devplaneapi/v1"
	"github.com/brevdev/dev-plane/gen/proto/go/devplaneapi/v1/devplaneapiv1connect"
	"github.com/brevdev/dev-plane/internal/apihelpers"
	"github.com/brevdev/dev-plane/internal/ids"
	"github.com/brevdev/dev-plane/pkg/collections"
	"github.com/brevdev/dev-plane/pkg/errors"
)

type %[2]sAPI struct {
	service Service
}

var _ devplaneapiv1connect.%[2]sServiceHandler = %[2]sAPI{}

func New%[2]sAPI(service Service) %[2]sAPI {
	return %[2]sAPI{service: service}
}

func (c %[2]sAPI) List%[2]s(ctx context.Context, request *connect.Request[devplaneapiv1.List%[2]sRequest]) (*connect.Response[devplaneapiv1.List%[2]sResponse], error) {
	// https://cloud.google.com/apis/design/standard_methods#list
	pp, err := apihelpers.MapAPIPageParamsToPageParams(request.Msg.PageParams)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	%[3]sPage, err := c.service.List%[2]s(ctx, (*List%[2]sOptions)(request.Msg.Options), pp)
	if err != nil {
		return nil, err
	}
	is, err := MapManyEntity%[2]ssToManyAPI%[2]ss(%[3]sPage.Items)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&devplaneapiv1.List%[2]sResponse{Items: is, NextPageToken: %[3]sPage.NextPageToken}), nil
}

func (c %[2]sAPI) Get%[2]s(ctx context.Context, request *connect.Request[devplaneapiv1.Get%[2]sRequest]) (*connect.Response[devplaneapiv1.Get%[2]sResponse], error) {
	// https://cloud.google.com/apis/design/standard_methods#get
	u, err := c.service.Get%[2]s(ctx, ids.%[2]sID(request.Msg.%[2]sId))
	if err != nil {
		return nil, err
	}
	i, err := MapEntity%[2]sToAPI%[2]s(u)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&devplaneapiv1.Get%[2]sResponse{%[2]s: i}), nil
}

func (c %[2]sAPI) Create%[2]s(ctx context.Context, request *connect.Request[devplaneapiv1.Create%[2]sRequest]) (*connect.Response[devplaneapiv1.Create%[2]sResponse], error) {
	// https://cloud.google.com/apis/design/standard_methods#create
	u, err := c.service.Create%[2]s(ctx, &Create%[2]sRequest{
		request.Msg,
	})
	if err != nil {
		return nil, err
	}
	i, err := MapEntity%[2]sToAPI%[2]s(u)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&devplaneapiv1.Create%[2]sResponse{%[2]s: i}), nil
}

func (c %[2]sAPI) Delete%[2]s(ctx context.Context, request *connect.Request[devplaneapiv1.Delete%[2]sRequest]) (*connect.Response[devplaneapiv1.Delete%[2]sResponse], error) {
	// https://cloud.google.com/apis/design/standard_methods#delete
	err := c.service.Delete%[2]s(ctx, ids.%[2]sID(request.Msg.%[2]sId))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&devplaneapiv1.Delete%[2]sResponse{}), nil
}

func MapAPI%[2]sToEntity%[2]s(%[3]s *devplaneapiv1.%[2]s) (%[2]s, error) {
	i, err := collections.TryCopyToNew[*devplaneapiv1.%[2]s, %[2]s](%[3]s)
	if err != nil {
		return %[2]s{}, errors.WrapAndTrace(err)
	}
	return i, nil
}

func MapEntity%[2]sToAPI%[2]s(%[3]s %[2]s) (*devplaneapiv1.%[2]s, error) {
	p, err := collections.TryCopyToNew[%[2]s, devplaneapiv1.%[2]s](%[3]s)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return &p, nil
}

func MapManyEntity%[2]ssToManyAPI%[2]ss(%[3]ss []%[2]s) ([]*devplaneapiv1.%[2]s, error) {
	api%[2]ss, err := collections.MapE(%[3]ss, MapEntity%[2]sToAPI%[2]s)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return api%[2]ss, nil
}
`

var apiTestTemplate = `
import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brevdev/dev-plane/ent"
	devplaneapiv1 "github.com/brevdev/dev-plane/gen/proto/go/devplaneapi/v1"
	"github.com/brevdev/dev-plane/gen/proto/go/devplaneapi/v1/devplaneapiv1connect"
	"github.com/brevdev/dev-plane/internal/labels"
	"github.com/brevdev/dev-plane/internal/testclient"
	"github.com/brevdev/dev-plane/pkg/commonentity"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TestClient struct {
	testclient.TestClient
	devplaneapiv1.%[2]sServiceClient
	Service Service
	API     %[2]sAPI
}

func NewTestClient(ctx context.Context, opts ...testclient.TestClientOption) TestClient {
	var service Service
	var api %[2]sAPI
	tc := testclient.NewTestClient(ctx, func(entClient *ent.Client) []testclient.HTTPServices {
		repository := NewEntRepository(entClient)
		service = NewService(repository, DummyWorkflowClient{})
		api = New%[2]sAPI(service)
		path, handler := devplaneapiv1connect.New%[2]sServiceHandler(api)
		return []testclient.HTTPServices{{
			Path:    path,
			Handler: handler,
		}}
	}, opts...)

	testClient := devplaneapiv1.New%[2]sServiceClient(tc.ClientConn)

	return TestClient{
		TestClient:         tc,
		%[2]sServiceClient: testClient,
		Service:            service,
		API:                api,
	}
}

func Test_Get%[2]s(t *testing.T) {
	_ = %[2]sAPI{}.Get%[2]s // allow easy go to def/ref while using protobuf clients

	ctx := context.Background()

	client := NewTestClient(ctx)
	defer client.Done()

	l := map[string]string{"label": "value"}
	%[3]s, err := client.Service.Create%[2]s(ctx, &Create%[2]sRequest{
		&devplaneapiv1.Create%[2]sRequest{
			Labels: l,
		},
	})
	if !assert.NoError(t, err) {
		return
	}

	resp, err := client.Get%[2]s(ctx, &devplaneapiv1.Get%[2]sRequest{
		%[2]sId: string(%[3]s.ID),
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, string(%[3]s.ID), resp.%[2]s.%[2]sId)
	assert.Equal(t, l, resp.%[2]s.Labels)

	_, err = client.Get%[2]s(ctx, &devplaneapiv1.Get%[2]sRequest{
		%[2]sId: "abbc",
	})
	assert.ErrorContains(t, err, "not found")
}

func Test_List%[2]s(t *testing.T) { //nolint:funlen // test
	_ = %[2]sAPI{}.List%[2]s // allow easy go to def/ref while using protobuf clients

	ctx := context.Background()

	client := NewTestClient(ctx)
	defer client.Done()

	pageSize := 3
	// get list with no %[3]ss in DB
	resp0, err := client.List%[2]s(ctx, &devplaneapiv1.List%[2]sRequest{
		PageParams: &devplaneapiv1.PageParams{
			PageSize: int32(pageSize),
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 0, len(resp0.Items))
	assert.Zero(t, resp0.NextPageToken)

	num%[2]ss := 10
	for i := 0; i < num%[2]ss; i++ {
		_, err = client.Service.Create%[2]s(ctx, &Create%[2]sRequest{
			&devplaneapiv1.Create%[2]sRequest{
				%[2]sId: fmt.Sprint(i),
				Labels:  map[string]string{"label": fmt.Sprint(i%%2)},
			},
		})
		if !assert.NoError(t, err) {
			fmt.Println(err)
			return
		}
	}

	// test can get page
	// ordering should be time based
	resp1, err := client.List%[2]s(ctx, &devplaneapiv1.List%[2]sRequest{
		PageParams: &devplaneapiv1.PageParams{
			PageSize: int32(pageSize),
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	if assert.Equal(t, pageSize, len(resp1.Items)) {
		assert.Equal(t, resp1.Items[0].%[2]sId, "0")
		assert.Equal(t, resp1.Items[1].%[2]sId, "1")
		assert.Equal(t, resp1.Items[2].%[2]sId, "2")
	}
	assert.NotZero(t, resp1.NextPageToken)

	// test can get next page
	resp2, err := client.List%[2]s(ctx, &devplaneapiv1.List%[2]sRequest{
		PageParams: &devplaneapiv1.PageParams{
			PageSize:  int32(pageSize),
			PageToken: resp1.NextPageToken,
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	if assert.Equal(t, pageSize, len(resp2.Items)) {
		assert.Equal(t, resp2.Items[0].%[2]sId, "3")
		assert.Equal(t, resp2.Items[1].%[2]sId, "4")
		assert.Equal(t, resp2.Items[2].%[2]sId, "5")
	}
	assert.NotEqualValues(t, resp1.Items, resp2.Items)

	// test page exact same size as total
	resp3, err := client.List%[2]s(ctx, &devplaneapiv1.List%[2]sRequest{
		PageParams: &devplaneapiv1.PageParams{
			PageSize: int32(num%[2]ss),
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, num%[2]ss, len(resp3.Items))
	assert.Contains(t, resp3.NextPageToken, "10")

	// test page more than total
	resp4, err := client.List%[2]s(ctx, &devplaneapiv1.List%[2]sRequest{
		PageParams: &devplaneapiv1.PageParams{
			PageSize: int32(num%[2]ss) + 5,
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, num%[2]ss, len(resp4.Items))
	assert.Contains(t, resp4.NextPageToken, "10")

	// test labels
	resp5, err := client.List%[2]s(ctx, &devplaneapiv1.List%[2]sRequest{
		Options: &devplaneapiv1.List%[2]sOptions{
			HasAllLabels: map[string]string{
				"label": "1",
			},
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, num%[2]ss/2, len(resp5.Items))
}

func Test_Create%[2]s(t *testing.T) {
	_ = %[2]sAPI{}.Create%[2]s // allow easy go to def/ref while using protobuf clients

	ctx := context.Background()

	client := NewTestClient(ctx)
	defer client.Done()

	req := &devplaneapiv1.Create%[2]sRequest{}
	resp, err := client.Create%[2]s(ctx, req)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotEmpty(t, resp.%[2]s.%[2]sId)
}

func Test_Create%[2]sWithProvidedID(t *testing.T) {
	_ = %[2]sAPI{}.Create%[2]s // allow easy go to def/ref while using protobuf clients

	ctx := context.Background()

	client := NewTestClient(ctx)
	defer client.Done()

	resp, err := client.Create%[2]s(ctx, &devplaneapiv1.Create%[2]sRequest{
		%[2]sId: "test-id",
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "test-id", resp.%[2]s.%[2]sId)
}

func Test_Delete%[2]s(t *testing.T) {
	_ = %[2]sAPI{}.Delete%[2]s // allow easy go to def/ref while using protobuf clients

	ctx := context.Background()

	client := NewTestClient(ctx)
	defer client.Done()

	%[3]s, err := client.Service.Create%[2]s(ctx, &Create%[2]sRequest{
		&devplaneapiv1.Create%[2]sRequest{},
	})
	if !assert.NoError(t, err) {
		return
	}

	_, err = client.Delete%[2]s(ctx, &devplaneapiv1.Delete%[2]sRequest{
		%[2]sId: string(%[3]s.ID),
	})
	if !assert.NoError(t, err) {
		return
	}

	time.Sleep(time.Millisecond * 50)
	_, err = client.Service.Get%[2]s(ctx, %[3]s.ID)
	assert.ErrorContains(t, err, "not found")
}

func Test_MapAPI%[2]sToEntity%[2]s(t *testing.T) {
	now := timestamppb.Now()
	p := devplaneapiv1.%[2]s{
		%[2]sId:    "id",
		CreateTime: now,
		UpdateTime: now,
		Labels:     map[string]string{"label": "1"},
	}
	e, err := MapAPI%[2]sToEntity%[2]s(&p)
	assert.NoError(t, err)
	assert.Equal(t, p.%[2]sId, string(e.ID))
	assert.Equal(t, p.CreateTime.AsTime(), e.CreateTime)
	assert.Equal(t, p.UpdateTime.AsTime(), e.UpdateTime)
	assert.Equal(t, labels.Labels(p.Labels), e.Labels)
}

func Test_MapEntity%[2]sToAPI%[2]s(t *testing.T) {
	now := time.Now()
	e := %[2]s{
		Time: commonentity.Time{
			CreateTime: now,
			UpdateTime: now,
		},
		ID:     "id",
		Labels: map[string]string{"label": "1"},
	}

	p, err := MapEntity%[2]sToAPI%[2]s(e)
	assert.NoError(t, err)
	assert.Equal(t, e.CreateTime.UTC(), p.CreateTime.AsTime().UTC())
	assert.Equal(t, e.UpdateTime.UTC(), p.UpdateTime.AsTime().UTC())
	assert.Equal(t, string(e.ID), p.%[2]sId)
	assert.Equal(t, e.Labels, labels.Labels(p.Labels))
}
`

var protoFileTemplate = `syntax = "proto3";

package %[1]s.%[2]s;

import "google/protobuf/timestamp.proto";
import "tagger/tagger.proto";
import "%[1]s/%[2]s/common.proto";


message %[3]s {
    string %[4]s_id = 1 [(tagger.tags) = "copier:\"ID\"" ];
    google.protobuf.Timestamp create_time = 2;
    google.protobuf.Timestamp update_time = 3;
    map<string, string> labels = 5;
}

message Get%[3]sRequest {
    string %[4]s_id = 1 [(tagger.tags) = "copier:\"ID\"" ];
}

message Get%[3]sResponse {
    %[3]s %[4]s = 1;
}

message List%[3]sRequest {
    PageParams page_params = 1;
    List%[3]sOptions options = 2;
}

message List%[3]sOptions {
    map<string, string> has_all_labels = 1;
    map<string, ListOfString> has_one_of_labels = 2;
    OrderOptions order_options = 3;
}

message List%[3]sResponse {
    repeated %[3]s items = 1;
    string next_page_token = 2;
}

message Create%[3]sRequest {
    string %[4]s_id = 1;
    map<string, string> labels = 2;
}

message Create%[3]sResponse {
    %[3]s %[4]s = 1;
}

message Delete%[3]sRequest {
    string %[4]s_id = 1 [(tagger.tags) = "copier:\"ID\"" ];
}

message Delete%[3]sResponse {
    // empty
}

service %[3]sService {
    rpc List%[3]s(List%[3]sRequest) returns (List%[3]sResponse);

    rpc Get%[3]s(Get%[3]sRequest) returns (Get%[3]sResponse);

    rpc Create%[3]s(Create%[3]sRequest) returns (Create%[3]sResponse);

    rpc Delete%[3]s(Delete%[3]sRequest) returns (Delete%[3]sResponse);
}

`

var entFileTemplate = `package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/mixin"
	"github.com/brevdev/dev-plane/internal/ids"
)

type %[1]s struct {
	ent.Schema
}

func (%[1]s) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.Time{},
		DeletedTimeMixin{},
		LabelsMixin{},
	}
}

func (%[1]s) Fields() []ent.Field {
	var id ids.%[1]sID
	return []ent.Field{
		MakeBrevID(id),
	}
}

func (%[1]s) Edges() []ent.Edge {
	return nil
}
`

var repoTestTemplate = `
import (
	"testing"
	"time"

	"github.com/brevdev/dev-plane/ent"
	"github.com/brevdev/dev-plane/pkg/commonentity"
	"github.com/stretchr/testify/assert"
)

func Test_MapEnt%[2]sTo%[2]s(t *testing.T) {
	now := time.Now()
	entObj := ent.%[2]s{
		ID:          "id",
		CreateTime:  now,
		UpdateTime:  now,
		DeletedTime: &now,
	}
	obj, err := MapEnt%[2]sTo%[2]s(&entObj)
	assert.NoError(t, err)
	assert.Equal(t, entObj.ID, obj.ID)
	assert.Equal(t, entObj.CreateTime, obj.CreateTime)
	assert.Equal(t, entObj.UpdateTime, obj.UpdateTime)
	assert.Equal(t, entObj.DeletedTime, obj.DeletedTime)
}

func Test_Map%[2]sToEnt%[2]s(t *testing.T) {
	now := time.Now()
	obj := %[2]s{
		ID: "id",
		Time: commonentity.Time{
			CreateTime:  now,
			UpdateTime:  now,
			DeletedTime: &now,
		},
	}
	entObj, err := Map%[2]sToEnt%[2]s(obj)
	assert.NoError(t, err)
	assert.Equal(t, obj.ID, entObj.ID)
	assert.Equal(t, obj.CreateTime, entObj.CreateTime)
	assert.Equal(t, obj.UpdateTime, entObj.UpdateTime)
	assert.Equal(t, obj.DeletedTime, entObj.DeletedTime)
}
`

var workflowFileTemplate = `
import (
	"context"
	"fmt"
	"time"

	"github.com/brevdev/dev-plane/internal/temporalhelpers"
	"github.com/brevdev/dev-plane/pkg/errors"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Temporal%[2]sWorkflowClient struct {
	temporalhelpers.WorkflowClient
}

func NewTemporal%[2]sWorkflowClient(temporalClient temporalhelpers.TemporalClient, registerer temporalhelpers.WorkflowRegisterer) Temporal%[2]sWorkflowClient {
	return Temporal%[2]sWorkflowClient{WorkflowClient: 
		temporalhelpers.WorkflowClient{
			TaskQueueName:  registerer.GetTaskQueueName(),
			TemporalClient: temporalClient,
			Registerer:     registerer,
	}}
}

func (w Temporal%[2]sWorkflowClient) Register(activities *Activities) error {
	err := w.RegisterWorkflow()
	if err != nil {
		return errors.WrapAndTrace(err)
	}

	RegisterActivities(w.Registerer, activities)

	return nil
}

func (w Temporal%[2]sWorkflowClient) RegisterWorkflow() error {
	err := Thing%[2]sWorkflow.Register(w.Registerer)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func RegisterActivities(registerer temporalhelpers.WorkflowRegisterer, activities *Activities) {
	registerer.RegisterActivityWithOptions(activities.Thing%[2]s, activity.RegisterOptions{})
}

var _ WorkflowClient = Temporal%[2]sWorkflowClient{}

func (w Temporal%[2]sWorkflowClient) ExecuteThing%[2]s(ctx context.Context) error {
	_, err := Thing%[2]sWorkflow.ExecuteWorkflow(ctx, 
		w.TemporalClient, 
		temporalclient.StartWorkflowOptions{},
		Thing%[2]sArgs{},
	)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

type Activities struct {
	service Service
}

var %[3]sActivities *Activities

func NewActivities(service Service) *Activities {
	return &Activities{service: service}
}

type Thing%[2]sArgs struct {
}

// Do not rename this function without some kind of migration
// Activity method must pointer receiver
func (a *Activities) Thing%[2]s(ctx context.Context, args Thing%[2]sArgs) (any, error) {
	_ = ctx
	_ = args
	return struct{}{}, nil
}

var Thing%[2]sWorkflow = temporalhelpers.MakeSingleActivityWorkflow(
	"DoThing%[2]sWorkflow",
	%[3]sActivities.Thing%[2]s,
	workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
		},
	})
`

const workflowTestFileTemplate = `
import (
	"context"
	"testing"

	"github.com/brevdev/dev-plane/internal/temporalhelpers"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

type WorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTestSuite))
}

func (s *WorkflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *WorkflowTestSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func (s *WorkflowTestSuite) Test_ExecuteThing%[2]sWorkflow() {
	tc := temporalhelpers.TestTemporalClient{Env: s.env}
	workflowClient := NewTemporal%[2]sWorkflowClient(tc, tc)
	err := workflowClient.RegisterWorkflow()
	if !s.NoError(err) {
		return
	}

	wasThing%[2]sCalled := false
	var a *Activities
	temporalhelpers.MockOnActivity(s.env, a.Thing%[2]s, func(_ context.Context, args Thing%[2]sArgs) (any, error) {
		wasThing%[2]sCalled = true
		return struct{}{}, nil
	})

	err = workflowClient.ExecuteThing%[2]s(context.Background())
	s.NoError(err)

	s.True(s.env.IsWorkflowCompleted())
	s.True(wasThing%[2]sCalled)
}

func Test_Thing%[2]sActivity(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	ctx := context.Background()
	tc := NewTestClient(ctx)
	activities := Activities{
		service: tc.Service,
	}

	env.RegisterActivity(activities.Thing%[2]s)

	_, err := temporalhelpers.ExecuteActivityTest(env, activities.Thing%[2]s, Thing%[2]sArgs{})
	require.NoError(t, err)
}
`
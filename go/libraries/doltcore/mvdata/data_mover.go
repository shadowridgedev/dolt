package mvdata

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
	"strings"
)

type MoveOperation string

const (
	OverwriteOp MoveOperation = "overwrite"
	UpdateOp    MoveOperation = "update"
	InvalidOp   MoveOperation = "invalid"
)

type MoveOptions struct {
	Operation   MoveOperation
	ContOnErr   bool
	SchFile     string
	MappingFile string
	PrimaryKey  string
	Src         *DataLocation
	Dest        *DataLocation
}

type DataMover struct {
	Rd         table.TableReadCloser
	Transforms *pipeline.TransformCollection
	Wr         table.TableWriteCloser
	ContOnErr  bool
}

type DataMoverCreationErrType string

const (
	CreateReaderErr DataMoverCreationErrType = "Create reader error"
	SchemaErr       DataMoverCreationErrType = "Schema error"
	MappingErr      DataMoverCreationErrType = "Mapping error"
	CreateMapperErr DataMoverCreationErrType = "Mapper creation error"
	CreateWriterErr DataMoverCreationErrType = "Create writer error"
	CreateSorterErr DataMoverCreationErrType = "Create sorter error"
)

type DataMoverCreationError struct {
	ErrType DataMoverCreationErrType
	Cause   error
}

func (dmce *DataMoverCreationError) String() string {
	return string(dmce.ErrType) + ": " + dmce.Cause.Error()
}

func NewDataMover(root *doltdb.RootValue, fs filesys.Filesys, mvOpts *MoveOptions) (*DataMover, *DataMoverCreationError) {
	var rd table.TableReadCloser
	var err error
	transforms := pipeline.NewTransformCollection()

	defer func() {
		if rd != nil {
			rd.Close()
		}
	}()

	rd, srcIsSorted, err := mvOpts.Src.CreateReader(root, fs)

	if err != nil {
		return nil, &DataMoverCreationError{CreateReaderErr, err}
	}

	outSch, err := getOutSchema(rd.GetSchema(), root, fs, mvOpts)

	if err != nil {
		return nil, &DataMoverCreationError{SchemaErr, err}
	}

	var mapping *rowconv.FieldMapping
	if mvOpts.MappingFile != "" {
		mapping, err = rowconv.MappingFromFile(mvOpts.MappingFile, fs, rd.GetSchema(), outSch)
	} else {
		mapping, err = rowconv.NewInferredMapping(rd.GetSchema(), outSch)
	}

	if err != nil {
		return nil, &DataMoverCreationError{MappingErr, err}
	}

	err = maybeMapFields(transforms, mapping)

	if err != nil {
		return nil, &DataMoverCreationError{CreateMapperErr, err}
	}

	var wr table.TableWriteCloser
	if mvOpts.Operation == OverwriteOp {
		wr, err = mvOpts.Dest.CreateOverwritingDataWriter(root, fs, srcIsSorted, outSch)
	} else {
		wr, err = mvOpts.Dest.CreateUpdatingDataWriter(root, fs, srcIsSorted, outSch)
	}

	if err != nil {
		return nil, &DataMoverCreationError{CreateWriterErr, err}
	}

	wr, err = maybeSort(wr, outSch, srcIsSorted, mvOpts)

	if err != nil {
		return nil, &DataMoverCreationError{CreateSorterErr, err}
	}

	imp := &DataMover{rd, transforms, wr, mvOpts.ContOnErr}
	rd = nil

	return imp, nil
}

func (imp *DataMover) Move() error {
	defer imp.Rd.Close()
	defer imp.Wr.Close()

	var rowErr error
	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		if !imp.ContOnErr {
			rowErr = trf
			return false
		}

		return true
	}

	p := pipeline.NewAsyncPipeline(pipeline.ProcFuncForReader(imp.Rd), pipeline.ProcFuncForWriter(imp.Wr), imp.Transforms, badRowCB)
	p.Start()

	err := p.Wait()

	if err != nil {
		return err
	}

	return rowErr
}

func maybeMapFields(transforms *pipeline.TransformCollection, mapping *rowconv.FieldMapping) error {
	rconv, err := rowconv.NewRowConverter(mapping)

	if err != nil {
		return err
	}

	if !rconv.IdentityConverter {
		transformer := pipeline.NewRowTransformer("Mapping transform", rowconv.GetRowConvTransformFunc(rconv))
		transforms.AppendTransforms(pipeline.NamedTransform{Name: "map", Func: transformer})
	}

	return nil
}

func maybeSort(wr table.TableWriteCloser, outSch schema.Schema, srcIsSorted bool, mvOpts *MoveOptions) (table.TableWriteCloser, error) {
	if !srcIsSorted && mvOpts.Dest.MustWriteSorted() {
		//TODO: implement this
		panic("Implement")
		//wr = table.NewSortingTableWriter(wr, outSch.GetPKIndex(), mvOpts.ContOnErr)
	}

	return wr, nil
}

func getOutSchema(inSch schema.Schema, root *doltdb.RootValue, fs filesys.ReadableFS, mvOpts *MoveOptions) (schema.Schema, error) {
	if mvOpts.Operation == UpdateOp {
		// Get schema from target
		rd, _, err := mvOpts.Dest.CreateReader(root, fs)

		if err != nil {
			return nil, err
		}

		defer rd.Close()

		return rd.GetSchema(), nil
	} else {
		sch, err := schFromFileOrDefault(mvOpts.SchFile, fs, inSch)

		if err != nil {
			return nil, err
		}

		sch, err = addPrimaryKey(sch, mvOpts.PrimaryKey)

		if err != nil {
			return nil, err
		}
		return sch, nil
	}

}

func schFromFileOrDefault(path string, fs filesys.ReadableFS, defSch schema.Schema) (schema.Schema, error) {
	if path != "" {
		data, err := fs.ReadFile(path)

		if err != nil {
			return nil, err
		}

		return encoding.UnmarshalJson(string(data))
	} else {
		return defSch, nil
	}
}

func addPrimaryKey(sch schema.Schema, explicitKey string) (schema.Schema, error) {
	if explicitKey != "" {
		keyCols := strings.Split(explicitKey, ",")
		trimmedCols := funcitr.MapStrings(keyCols, func(s string) string { return strings.TrimSpace(s) })
		keyColSet := set.NewStrSet(trimmedCols)

		foundPKCols := 0
		var updatedCols []schema.Column

		sch.GetAllCols().ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
			if keyColSet.Contains(col.Name) {
				foundPKCols++
				col.IsPartOfPK = true
				col.Constraints = []schema.ColConstraint{schema.NotNullConstraint{}}
			} else {
				col.IsPartOfPK = false
				col.Constraints = nil
			}

			updatedCols = append(updatedCols, col)
			return false
		})

		if keyColSet.Size() != foundPKCols {
			return nil, errors.New("could not find all pks: " + explicitKey)
		}

		updatedColColl, err := schema.NewColCollection(updatedCols...)

		if err != nil {
			return nil, err
		}

		return schema.SchemaFromCols(updatedColColl), nil
	}

	return sch, nil
}